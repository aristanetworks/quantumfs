// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the small/medium file transitions and medium file operations

import "bytes"
import "io"
import "io/ioutil"
import "os"
import "testing"
import "syscall"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"

func TestMedBranch(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		// generate data larger than small file
		data := genData(int(quantumfs.MaxSmallFileSize()) +
			quantumfs.MaxBlockSize)
		// Write the data to the file continually past what
		// a single block could hold.
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing data to new fd: %v",
			err)

		// Branch the workspace
		dst := test.branchWorkspace(workspace)
		test.Assert(err == nil, "Unable to branch")

		// use a stride offset such that at least 20 checks
		// are done
		test.CheckSparse(test.absPath(dst+"/test"), testFilename,
			len(data)/20, 10)
	})
}

func TestFileExpansion(t *testing.T) {
	// generate more data than small file size
	data := genData(int(quantumfs.MaxSmallFileSize()) +
		quantumfs.MaxBlockSize)
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		// Write the data sequence to the file continually past what
		// a single block could hold.
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing data to new fd: %v",
			err)

		// Read it back
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading data back from file")
		test.Assert(len(data) == len(output),
			"Data length mismatch, %d vs %d", len(data), len(output))

		if !bytes.Equal(data, output) {
			for i := 0; i < len(data); i++ {
				test.Assert(data[i] == output[i],
					"Data readback mismatch at idx %d, %s vs %s",
					i, data[i], output[i])
			}
		}

		// Test that we can truncate to any offset lower
		// than len(data) within same file type
		newLen := len(data) - 1024
		os.Truncate(testFilename, int64(newLen))

		// Ensure that the data remaining is what we expect
		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading data from file")
		test.Assert(len(output) == newLen, "Truncated length incorrect")
		test.Assert(bytes.Equal(data[:newLen], output),
			"Post-truncation mismatch")

		// Let's re-expand it using SetAttr to any size
		// greater than newLen
		truncLen := newLen + quantumfs.MaxBlockSize
		os.Truncate(testFilename, int64(truncLen))

		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading data+hole back from file")
		test.Assert(bytes.Equal(data[:newLen], output[:newLen]),
			"Data readback mismatch")
		delta := truncLen - newLen
		test.Assert(bytes.Equal(make([]byte, delta), output[newLen:]),
			"File hole not filled with zeros")
	})
}

func TestMedFileAttr(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		api := test.getApi()

		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to medium file size
		newSize := quantumfs.MaxSmallFileSize() +
			uint64(quantumfs.MaxBlockSize)
		os.Truncate(testFilename, int64(newSize))

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Unable to stat file: %v", err)
		test.Assert(stat.Size == int64(newSize), "File size incorrect, %d",
			stat.Size)

		// Read what should be all zeros
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading hole from file")

		for i := 0; i < len(output); i++ {
			test.Assert(output[i] == 0, "Data not zeroed in file, %s",
				output[i])
		}

		// Ensure that we can write data into the hole
		// hole is from 0 to EOF
		testString := []byte("testData")
		var file *os.File
		var count int
		dataOffset := quantumfs.MaxBlockSize
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.Assert(err == nil, "Unable to open file for rdwr: %v", err)
		count, err = file.WriteAt(testString, int64(dataOffset))
		test.Assert(err == nil, "Unable to write at offset: %v", err)
		test.Assert(count == len(testString),
			"Unable to write data all at once")
		err = file.Close()
		test.Assert(err == nil, "Unable to close file handle")

		// Branch the workspace
		dst := "dst/medattrsparse/test"
		err = api.Branch(test.relPath(workspace), dst)
		test.Assert(err == nil, "Unable to branch")

		// stride offset chosen to allow 20 checks
		test.CheckSparse(test.absPath(dst+"/test"), testFilename,
			int(newSize)/20, 10)
	})
}

func TestMedFileZero(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		data := genData(10 * 1024)
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing tiny data to new fd")
		// expand this to be medium file type
		os.Truncate(testFilename, int64(quantumfs.MaxSmallFileSize())+
			int64(quantumfs.MaxBlockSize))

		os.Truncate(testFilename, 0)
		test.Assert(test.FileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.Assert(len(output) == 0, "Empty file not really empty")
		test.Assert(err == nil, "Unable to read from empty file")
	})
}

func TestMultiBlockFileReadPastEnd(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)

		// generate small file (<= 1 block)
		testDataSize := quantumfs.MaxBlockSize
		maxTestDataSize := 100 * 1024
		if testDataSize > maxTestDataSize {
			testDataSize = maxTestDataSize
		}
		data := genData(testDataSize)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		// expand the file to multi-block file
		os.Truncate(testFilename, int64(2*quantumfs.MaxBlockSize))

		// Then confirm we can read back past the data and get the correct
		// EOF return value.
		input := make([]byte, 100*1024)
		_, err = file.ReadAt(input, int64(3*quantumfs.MaxBlockSize))
		test.Assert(err == io.EOF, "Expected EOF got: %v", err)

		file.Close()
	})
}
