// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the very large file transitions

import "bytes"
import "os"
import "testing"

func TestSmallConvert_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		data := genFibonacci(512 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil,
			"Error writing 0.5MB fibonacci to new fd: %v", err)

		// 49GB sparse file incoming
		newLen := 49 * 1024 * 1024 * 1024
		os.Truncate(testFilename, int64(newLen))
		test.assert(test.fileSize(testFilename) == int64(newLen),
			"Truncation expansion failed")

		// Then append fib *again* to the end of it
		err = printToFile(testFilename, string(data))
		test.assert(err == nil,
			"Error writing 0.5MB fibonacci to existing fd: %v", err)
		test.assert(test.fileSize(testFilename) == int64(newLen + len(data)),
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
	})
}
