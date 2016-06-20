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
		data := genFibonacci(40 * 1024 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing 40MB fibonacci to new fd: %v",
			err)

		// Read it back
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 40MB fibonacci back from file")
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

		// Ensure that the data remaining is what we expect
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 2.5MB fibonacci from file")
		test.assert(len(output) == newLen, "Truncated length incorrect")
		test.assert(bytes.Equal(data[:newLen], output),
			"Post-truncation mismatch")

		// Let's re-expand it using SetAttr
		os.Truncate(testFilename, 40*1024*1024)

		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading fib+hole back from file")
		test.assert(bytes.Equal(data[:newLen], output[:newLen]),
			"Data readback mismatch")
		delta := (40 * 1024 * 1024) - newLen
		test.assert(bytes.Equal(make([]byte, delta), output[newLen:]),
			"File hole not filled with zeros")
	})
}

func TestLargeFileAttr_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to large file size
		newSize := int64(40 * 1024 * 1024)
		os.Truncate(testFilename, newSize)

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Unable to stat file: %v", err)
		test.assert(stat.Size == newSize, "File size incorrect, %d",
			stat.Size)

		// Read what should be 40MB of zeros
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 40MB hole from file")

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
	})
}
