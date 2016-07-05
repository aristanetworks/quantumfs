// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the small/medium/large file transitions and large file operations

import "bytes"
import "io/ioutil"
import "os"
import "testing"
import "syscall"

func TestLargeFileExpansion_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		// Write the fibonacci sequence to the file continually past what
		// a medium file could hold.
		data := genFibonacci(34 * 1024 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing 34MB fibonacci to new fd: %v",
			err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Unable to stat file: %v", err)
		test.assert(stat.Size == int64(len(data)), "File size incorrect, %d",
			stat.Size)

		// Read it back
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 34MB fibonacci from file")
		test.assert(len(data) == len(output),
			"Data length mismatch, %d vs %d", len(data), len(output))
		if !bytes.Equal(data, output) {
			for i := 0; i < len(data); i += 1024 {
				test.assert(data[i] == output[i],
					"Data readback mismatch at idx %d, %s vs %s",
					i, data[i], output[i])
			}
		}

		// Test that we can truncate this
		newLen := 2500000
		os.Truncate(testFilename, int64(newLen))

		// Ensure that the data ends where we expect
		offset := 2432132
		fd, fdErr := os.OpenFile(testFilename, os.O_RDONLY, 0777)
		test.assert(fdErr == nil, "Unable to open file for RDONLY")
		// Try to read more than should exist
		endOfFile := test.readTo(fd, offset, len(data)-offset)
		err = fd.Close()
		test.assert(err == nil, "Unable to close file")
		test.assert(len(endOfFile) == newLen-offset, "Truncation incorrect")
		test.assert(bytes.Equal(data[offset:offset+len(endOfFile)],
			endOfFile), "Post-truncation mismatch")

		// Let's re-expand it using SetAttr
		os.Truncate(testFilename, int64(len(data)))

		fd, fdErr = os.OpenFile(testFilename, os.O_RDONLY, 0777)
		test.assert(fdErr == nil, "Unable to open for for RDONLY")
		endOfFile = test.readTo(fd, offset, len(data)-offset)
		err = fd.Close()
		test.assert(err == nil, "Unable to close file")
		copy(output[offset:], endOfFile)
		test.assert(bytes.Equal(data, output), "Data and file mismatch")
	})
}

func TestLargeFileAttr_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to large file size
		newSize := int64(34 * 1024 * 1024)
		os.Truncate(testFilename, newSize)

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Unable to stat file: %v", err)
		test.assert(stat.Size == newSize, "File size incorrect, %d",
			stat.Size)

		// Read what should be 34MB of zeros
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 34MB hole from file")

		for i := 0; i < len(output); i += 1024 {
			test.assert(output[i] == 0, "Data not zeroed in file, %s",
				output[i])
		}

		// Ensure that we can write data into the hole
		testString := []byte("testData")
		var file *os.File
		var count int
		dataOffset := 4500000
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.assert(err == nil, "Unable to open file for rdwr: %v", err)
		count, err = file.WriteAt(testString, int64(dataOffset))
		test.assert(err == nil, "Unable to write at offset: %v", err)
		test.assert(count == len(testString),
			"Unable to write data all at once")
		err = file.Close()
		test.assert(err == nil, "Unable to close file handle")

		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Failed to read large file with sparse data")
		test.assert(bytes.Equal(output[dataOffset:dataOffset+
			len(testString)], testString),
			"Offset write failed in sparse file")

		// Branch the workspace
		dst := "largeattrsparse/test"
		err = api.Branch(test.relPath(workspace), dst)
		test.assert(err == nil, "Unable to branch")

		test.checkSparse(test.absPath(dst+"/test"), testFilename, 250000,
			10)
	})
}

func TestLargeFileZero_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		data := genFibonacci(10 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing tiny fib to new fd")
		// expand this to be the desired file type
		os.Truncate(testFilename, 34 * 1048576)

		os.Truncate(testFilename, 0)
		test.assert(test.fileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.assert(len(output) == 0, "Empty file not really empty")
		test.assert(err == nil, "Unable to read from empty file")
	})
}
