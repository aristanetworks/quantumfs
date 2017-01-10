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

func TestMedBranch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		data := genData(4 * 1024 * 1024)
		// Write the data to the file continually past what
		// a single block could hold.
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing 4MB data to new fd: %v",
			err)

		// Branch the workspace
		dst := test.branchWorkspace(workspace)
		test.assert(err == nil, "Unable to branch")

		test.checkSparse(test.absPath(dst+"/test"), testFilename, 40000,
			10)
	})
}

func TestFileExpansion(t *testing.T) {
	data := genData(4 * 1024 * 1024)
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		// Write the data sequence to the file continually past what
		// a single block could hold.
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing 4MB data to new fd: %v",
			err)

		// Read it back
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 4MB data back from file")
		test.assert(len(data) == len(output),
			"Data length mismatch, %d vs %d", len(data), len(output))

		if !bytes.Equal(data, output) {
			for i := 0; i < len(data); i++ {
				test.assert(data[i] == output[i],
					"Data readback mismatch at idx %d, %s vs %s",
					i, data[i], output[i])
			}
		}

		// Test that we can truncate this
		newLen := 2500000
		os.Truncate(testFilename, int64(newLen))

		// Ensure that the data remaining is what we expect
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 2.5MB data from file")
		test.assert(len(output) == newLen, "Truncated length incorrect")
		test.assert(bytes.Equal(data[:newLen], output),
			"Post-truncation mismatch")

		// Let's re-expand it using SetAttr
		const truncLen = 5 * 1024 * 1024
		os.Truncate(testFilename, truncLen)

		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading data+hole back from file")
		test.assert(bytes.Equal(data[:newLen], output[:newLen]),
			"Data readback mismatch")
		delta := truncLen - newLen
		test.assert(bytes.Equal(make([]byte, delta), output[newLen:]),
			"File hole not filled with zeros")
	})
}

func TestMedFileAttr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to medium file size
		newSize := int64(2 * 1024 * 1024)
		os.Truncate(testFilename, newSize)

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Unable to stat file: %v", err)
		test.assert(stat.Size == newSize, "File size incorrect, %d",
			stat.Size)

		// Read what should be 2MB of zeros
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 4MB hole from file")

		for i := 0; i < len(output); i++ {
			test.assert(output[i] == 0, "Data not zeroed in file, %s",
				output[i])
		}

		// Ensure that we can write data into the hole
		testString := []byte("testData")
		var file *os.File
		var count int
		dataOffset := 1500000
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.assert(err == nil, "Unable to open file for rdwr: %v", err)
		count, err = file.WriteAt(testString, int64(dataOffset))
		test.assert(err == nil, "Unable to write at offset: %v", err)
		test.assert(count == len(testString),
			"Unable to write data all at once")
		err = file.Close()
		test.assert(err == nil, "Unable to close file handle")

		// Branch the workspace
		dst := "dst/medattrsparse/test"
		err = api.Branch(test.relPath(workspace), dst)
		test.assert(err == nil, "Unable to branch")

		test.checkSparse(test.absPath(dst+"/test"), testFilename, 25000,
			10)
	})
}

func TestMedFileZero(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		data := genData(10 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing tiny data to new fd")
		// expand this to be the desired file type
		os.Truncate(testFilename, 2*1048576)

		os.Truncate(testFilename, 0)
		test.assert(test.fileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.assert(len(output) == 0, "Empty file not really empty")
		test.assert(err == nil, "Unable to read from empty file")
	})
}

func TestMultiBlockFileReadPastEnd(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)

		data := genData(100 * 1024)
		_, err = file.Write(data)
		test.assert(err == nil, "Error writing data to file: %v", err)

		os.Truncate(testFilename, 3*1024*1024)

		// Then confirm we can read back past the data and get the correct
		// EOF return value.
		input := make([]byte, 100*1024)
		_, err = file.ReadAt(input, 10*1024*1024)
		test.assert(err == io.EOF, "Expected EOF got: %v", err)

		file.Close()
	})
}
