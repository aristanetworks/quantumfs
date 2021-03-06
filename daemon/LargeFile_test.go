// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test the small/medium/large file transitions and large file operations

import (
	"bytes"
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
)

func TestLargeFileRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename := workspace + "/test"

		// Write the data sequence to the file continually past what
		// a medium file could hold.
		maxMediumFileSize := int(quantumfs.MaxMediumFileSize())
		offset := maxMediumFileSize - 2*quantumfs.MaxBlockSize
		fullLength := maxMediumFileSize + 2*quantumfs.MaxBlockSize
		data := GenData(fullLength - offset)
		err := testutils.PrintToFile(testFilename, string(data[0]))
		test.Assert(err == nil, "Error creating file")
		os.Truncate(testFilename, int64(offset))
		err = testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing into medium sparse file: %v",
			err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Unable to stat file: %v", err)
		test.Assert(stat.Size == int64(fullLength),
			"File size incorrect, %d", stat.Size)

		// Read a sample of it back
		fd, fdErr := os.OpenFile(testFilename, os.O_RDONLY, 0777)
		test.Assert(fdErr == nil, "Unable to open file for RDONLY")
		// Try to read more than should exist
		endOfFile := test.ReadTo(fd, offset, len(data)*2)
		err = fd.Close()
		test.Assert(err == nil, "Unable to close file")
		test.Assert(bytes.Equal(data[:len(endOfFile)],
			endOfFile), "Data expansion corruption in file contents")
	})
}

func TestLargeFileExpansion(t *testing.T) {
	runExpensiveTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/test"

		// Write the data sequence to the file continually past what
		// a medium file could hold.
		maxMediumFileSize := int(quantumfs.MaxMediumFileSize())
		offset := maxMediumFileSize - 2*quantumfs.MaxBlockSize
		fullLength := maxMediumFileSize + 2*quantumfs.MaxBlockSize
		data := GenData(fullLength - offset)
		// write 4 blocks at the start of file
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error creating file")
		os.Truncate(testFilename, int64(offset))
		// write 4 blocks at offset creating a large file
		err = testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing into medium sparse file: %v",
			err)

		// Test that we can truncate this large file
		// to a medium file
		newLen := int(quantumfs.MaxSmallFileSize()) +
			quantumfs.MaxBlockSize
		os.Truncate(testFilename, int64(newLen))

		// Ensure that the data ends where we expect
		// offset is < newLen and < len(data)
		offset = newLen - 1024
		fd, fdErr := os.OpenFile(testFilename, os.O_RDONLY, 0777)
		test.Assert(fdErr == nil, "Unable to open file for RDONLY")
		// Try to read more than should exist
		endOfFile := test.ReadTo(fd, offset, len(data))
		err = fd.Close()
		test.Assert(err == nil, "Unable to close file")
		test.Assert(len(endOfFile) == newLen-offset, "Truncation incorrect")
		test.Assert(bytes.Equal(data[offset:offset+len(endOfFile)],
			endOfFile), "Post-truncation mismatch")

		// Let's re-expand it using SetAttr
		os.Truncate(testFilename, int64(len(data)))

		fd, fdErr = os.OpenFile(testFilename, os.O_RDONLY, 0777)
		test.Assert(fdErr == nil, "Unable to open for for RDONLY")
		endOfFile = test.ReadTo(fd, offset, (newLen-offset)+1000)
		err = fd.Close()
		test.Assert(err == nil, "Unable to close file")
		allZeroes := true
		test.Assert(endOfFile[newLen-offset-1] != 0, "Data zeros offset")
		for i := newLen - offset; i < len(endOfFile); i += 1000 {
			if endOfFile[i] != 0 {
				allZeroes = false
				break
			}
		}
		test.Assert(allZeroes, "Data hole isn't all zeroes")
		if !bytes.Equal(data[offset:newLen], endOfFile[:newLen-offset]) {
			for i := 0; i < newLen-offset; i++ {
				test.Assert(data[offset+i] == endOfFile[i],
					"byte mismatch %d %v %v", i, data[i],
					endOfFile[i])
			}
			test.Assert(false, "Arrays not equal, but debug failed")
		}
	})
}

func TestLargeFileAttr(t *testing.T) {
	runExpensiveTest(t, func(test *testHelper) {
		api := test.getApi()

		workspace := test.NewWorkspace()

		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to large file size
		newSize := int64(quantumfs.MaxMediumFileSize()) +
			int64(quantumfs.MaxBlockSize)
		os.Truncate(testFilename, newSize)

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Unable to stat file: %v", err)
		test.Assert(stat.Size == newSize, "File size incorrect, %d",
			stat.Size)

		// Read what should be all zeros
		// stride offset to cause 1000 checks
		test.checkZeroSparse(testFilename, int(newSize)/1000)

		// Ensure that we can write data into the hole
		testString := []byte("testData")
		var file *os.File
		var count int
		// hole exists from offset 0 to EOF
		dataOffset := 2 * quantumfs.MaxBlockSize
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.Assert(err == nil, "Unable to open file for rdwr: %v", err)
		count, err = file.WriteAt(testString, int64(dataOffset))
		test.Assert(err == nil, "Unable to write at offset: %v", err)
		test.Assert(count == len(testString),
			"Unable to write data all at once")

		output := make([]byte, len(testString))
		_, err = file.ReadAt(output, int64(dataOffset))
		err = file.Close()
		test.Assert(err == nil, "Unable to close file handle")

		test.Assert(err == nil, "Failed to read large file with sparse data")
		test.Assert(bytes.Equal(output, testString),
			"Offset write failed in sparse file")

		// Branch the workspace
		dst := "dst/largeattrsparse/test"
		err = api.Branch(test.RelPath(workspace), dst)
		test.Assert(err == nil, "Unable to branch")

		test.checkSparse(test.AbsPath(dst+"/test"), testFilename,
			int(newSize)/100, 10)
	})
}

func TestLargeFileZero(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/test"

		data := GenData(10 * 1024)
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing tiny data to new fd")
		// expand this to Large file type
		os.Truncate(testFilename, int64(quantumfs.MaxMediumFileSize())+
			int64(quantumfs.MaxBlockSize))

		os.Truncate(testFilename, 0)
		test.Assert(test.FileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.Assert(len(output) == 0, "Empty file not really empty")
		test.Assert(err == nil, "Unable to read from empty file")
	})
}
