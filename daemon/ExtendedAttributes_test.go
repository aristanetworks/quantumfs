// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on extended attributes

import "bytes"
import "os"
import "syscall"
import "testing"

func TestExtendedAttrReadWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		attrNoData := "user.nodata"
		attrData := "user.data"
		attrDataData := []byte("extendedattributedata")

		err = syscall.Setxattr(testFilename, attrNoData, []byte{}, 0)
		test.assert(err == nil, "Error setting nodata XAttr: %v", err)

		err = syscall.Setxattr(testFilename, attrData, attrDataData, 0)
		test.assert(err == nil, "Error setting data XAttr: %v", err)

		// Now confirm we can read them out again
		data := make([]byte, 100)

		size, err := syscall.Getxattr(testFilename, attrNoData, data)
		test.assert(err == nil, "Error reading nodata XAttr: %v", err)
		test.assert(size == 0, "nodata XAttr size not zero: %d", size)

		size, err = syscall.Getxattr(testFilename, attrData, data)
		test.assert(err == nil, "Error reading data XAttr: %v", err)
		test.assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data, attrDataData)
	})
}
