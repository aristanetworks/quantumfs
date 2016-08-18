// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the very large file transitions

import "bytes"
import "io"
import "io/ioutil"
import "os"
import "strconv"
import "testing"

func runConvertFrom(test *testHelper, fromFileSize uint64) {
	test.startDefaultQuantumFs()

	workspace := test.nullWorkspace()
	testFilename := workspace + "/test"

	data := genFibonacci(4096)
	err := printToFile(testFilename, string(data))
	test.assert(err == nil,
		"Error writing 4kB fibonacci to new fd: %v", err)
	// Make this the file size desired (and hence type)
	err = os.Truncate(testFilename, int64(fromFileSize))
	test.assert(err == nil, "Unable to truncate expand file to %d", fromFileSize)

	// 200GB sparse file incoming
	newLen := 200 * 1024 * 1024 * 1024
	os.Truncate(testFilename, int64(newLen))
	test.assert(test.fileSize(testFilename) == int64(newLen),
		"Truncation expansion failed")

	// Then append fib *again* to the end of it
	err = printToFile(testFilename, string(data))
	test.assert(err == nil,
		"Error writing 0.5MB fibonacci to existing fd: %v", err)
	test.assert(test.fileSize(testFilename) == int64(newLen+len(data)),
		"Post truncation expansion write failed, %d",
		test.fileSize(testFilename))

	// Read our fib back
	fd, fdErr := os.OpenFile(testFilename, os.O_RDONLY, 0777)
	test.assert(fdErr == nil, "Unable to open file for RDONLY")
	// Try to read more than should exist
	startData := test.readTo(fd, 0, len(data)+1000)
	err = fd.Close()
	test.assert(err == nil, "Unable to close file")
	test.assert(bytes.Equal(data, startData[:len(data)]),
		"Data not preserved in small to very large expansion")
	test.assert(bytes.Equal(startData[len(data):], make([]byte, 1000)),
		"Data hole not created in small to very large file expand")

	// Now check the fib at the end of the file
	fd, fdErr = os.OpenFile(testFilename, os.O_RDONLY, 0777)
	test.assert(fdErr == nil, "Unable to open file for RDONLY")
	// Try to read more than should exist
	endData := test.readTo(fd, newLen-1000, len(data)+2000)
	err = fd.Close()
	test.assert(err == nil, "Unable to close file")
	test.assert(bytes.Equal(endData[:1000], make([]byte, 1000)),
		"Data hole not in small to very large file expansion")
	test.assert(bytes.Equal(data, endData[1000:]),
		"Data entry error after small to very large expansion")

	// Branch the workspace
	api := test.getApi()
	dst := strconv.FormatUint(fromFileSize, 10) + "verylargeattrsparse/test"
	err = api.Branch(test.relPath(workspace), dst)
	test.assert(err == nil, "Unable to branch")

	test.checkSparse(test.absPath(dst+"/test"), testFilename, newLen/100, 10)
}

func TestSmallConvert_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, 512*1024)
	})
}

func TestMedConvert_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, 7*1024*1024)
	})
}

func TestLargeConvert_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, 44*1024*1024)
	})
}

func TestVeryLargeFileZero_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		data := genFibonacci(10 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing tiny fib to new fd")
		// expand this to be the desired file type
		os.Truncate(testFilename, 60*1024*1024*1024)

		os.Truncate(testFilename, 0)
		test.assert(test.fileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.assert(len(output) == 0, "Empty file not really empty")
		test.assert(err == nil, "Unable to read from empty file")
	})
}

func TestVeryLargeFileReadPastEnd(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)

		data := genFibonacci(100 * 1024)
		_, err = file.Write(data)
		test.assert(err == nil, "Error writing data to file: %v", err)

		os.Truncate(testFilename, 300*1024*1024*1024)

		// Then confirm we can read back past the data and get the correct
		// EOF return value.
		input := make([]byte, 100*1024)
		_, err = file.ReadAt(input, 1000*1024*1024*1024)
		test.assert(err == io.EOF, "Expected EOF got: %v", err)

		file.Close()
	})
}
