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

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"

func runConvertFrom(test *testHelper, fromFileSize uint64) {
	workspace := test.NewWorkspace()

	testFilename := workspace + "/test"

	// fromFileSize must be > testDataSize
	testDataSize := 4096
	data := GenData(testDataSize)
	err := testutils.PrintToFile(testFilename, string(data))
	test.Assert(err == nil,
		"Error writing data (%d bytes) to new fd: %v",
		testDataSize, err)
	// Make this the file size desired (and hence type)
	err = os.Truncate(testFilename, int64(fromFileSize))
	test.Assert(err == nil, "Unable to truncate expand file to %d", fromFileSize)

	// a sparse VeryLargeFile
	newLen := int(quantumfs.MaxLargeFileSize()) + quantumfs.MaxBlockSize
	// upper limit of 200GB
	maxLen := 200 * 1024 * 1024 * 1024
	if newLen > maxLen {
		newLen = maxLen
	}
	os.Truncate(testFilename, int64(newLen))
	test.Assert(test.FileSize(testFilename) == int64(newLen),
		"Truncation expansion failed")

	// Then append data *again* to the end of it
	err = testutils.PrintToFile(testFilename, string(data))
	test.Assert(err == nil,
		"Error writing data (%d bytes) to existing fd: %v",
		testDataSize, err)
	test.Assert(test.FileSize(testFilename) == int64(newLen+testDataSize),
		"Post truncation expansion write failed, %d",
		test.FileSize(testFilename))

	// Read our data back
	fd, fdErr := os.OpenFile(testFilename, os.O_RDONLY, 0777)
	test.Assert(fdErr == nil, "Unable to open file for RDONLY")
	// Try to read more than should exist
	startData := test.ReadTo(fd, 0, testDataSize+1000)
	err = fd.Close()
	test.Assert(err == nil, "Unable to close file")
	test.Assert(bytes.Equal(data, startData[:testDataSize]),
		"Data not preserved in small to very large expansion")
	test.Assert(bytes.Equal(startData[testDataSize:], make([]byte, 1000)),
		"Data hole not created in small to very large file expand")

	// Now check the data at the end of the file
	fd, fdErr = os.OpenFile(testFilename, os.O_RDONLY, 0777)
	test.Assert(fdErr == nil, "Unable to open file for RDONLY")
	// Try to read more than should exist
	endData := test.ReadTo(fd, newLen-1000, testDataSize+2000)
	err = fd.Close()
	test.Assert(err == nil, "Unable to close file")
	test.Assert(bytes.Equal(endData[:1000], make([]byte, 1000)),
		"Data hole not in small to very large file expansion")
	test.Assert(bytes.Equal(data, endData[1000:]),
		"Data entry error after small to very large expansion")

	// Branch the workspace
	api := test.getApi()
	dst := strconv.FormatUint(fromFileSize, 10) + "dst/verylargeattrsparse/test"
	err = api.Branch(test.RelPath(workspace), dst)
	test.Assert(err == nil, "Unable to branch")

	test.checkSparse(test.absPath(dst+"/test"), testFilename, newLen/100, 10)
}

func TestSmallConvert(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, quantumfs.MaxSmallFileSize())
	})
}

func TestMaxSmallConvert(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, uint64(quantumfs.MaxBlockSize))
	})
}

func TestMedConvert(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, quantumfs.MaxSmallFileSize()+
			uint64(quantumfs.MaxBlockSize))
	})
}

func TestLargeConvert(t *testing.T) {
	runTest(t, func(test *testHelper) {
		runConvertFrom(test, quantumfs.MaxMediumFileSize()+
			uint64(quantumfs.MaxBlockSize))
	})
}

func TestVeryLargeFileZero(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/test"

		tinyDataSize := 10 * 1024
		data := GenData(tinyDataSize)
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing tiny (%d) data to new fd",
			tinyDataSize)
		// expand this to be the desired file type (VeryLargeFile)
		os.Truncate(testFilename, int64(quantumfs.MaxLargeFileSize())+
			int64(quantumfs.MaxBlockSize))

		os.Truncate(testFilename, 0)
		test.Assert(test.FileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.Assert(len(output) == 0, "Empty file not really empty")
		test.Assert(err == nil, "Unable to read from empty file")
	})
}

func TestVeryLargeFileReadPastEnd(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)

		testDataSize := 100 * 1024
		data := GenData(testDataSize)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		// expand to VeryLargeFile type
		os.Truncate(testFilename, int64(quantumfs.MaxLargeFileSize())+
			int64(quantumfs.MaxBlockSize))

		// Then confirm we can read back past the data and get the correct
		// EOF return value.
		input := make([]byte, testDataSize)
		_, err = file.ReadAt(input, int64(quantumfs.MaxLargeFileSize())+
			int64(2*quantumfs.MaxBlockSize))
		test.Assert(err == io.EOF, "Expected EOF got: %v", err)

		file.Close()
	})
}
