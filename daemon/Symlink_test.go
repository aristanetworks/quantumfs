// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on symlinks, only contains: CreateSymlink and
// extended attributes related operations: set, get, list, remove

import "bytes"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/utils"

// create a source file and link it to a symlink file
func createSymlink(workspace string, test *testHelper) string {
	targetFilename := workspace + "/target"
	fd, err := syscall.Creat(targetFilename, 0124)
	test.Assert(err == nil, "Error creating file: %v", err)

	err = syscall.Close(fd)
	test.Assert(err == nil, "Error closing fd: %v", err)

	linkFilename := workspace + "/symlink"
	err = syscall.Symlink(targetFilename, linkFilename)
	test.Assert(err == nil, "Error creating symlink: %v", err)
	return linkFilename
}

// Verify the creation of symlink file
func TestSymlinkCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		// create a symlink file
		linkFilename := createSymlink(workspace, test)

		// test the status of the symlink file
		var stat syscall.Stat_t
		err := syscall.Lstat(linkFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)
	})
}

// Verify the set/get XAttr function for Symlink's own Extended Attributes
func TestSymlinkXAttrSetGet(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		symlFilename := createSymlink(workspace, test)

		const size = 32
		data := []byte("TestOne")
		data2 := []byte("TestTwo")

		// verify the forbidden namespace "user"
		// Test the consistance b/t Unix and QuantumFS
		// The reference about namespace can be found on
		// http://man7.org/linux/man-pages/man7/xattr.7.html
		err := utils.LSetXattr(symlFilename, "user.one", data, 0)
		test.Assert(err != nil,
			"Error setting XAttr not supposed to run")

		// verify the normal namespace "security"
		err = utils.LSetXattr(symlFilename, "security.one", data, 0)
		test.Assert(err == nil, "Error setting XAttr: %v", err)

		// verify the attribute is actually stored
		_, err, output := utils.LGetXattr(symlFilename, "security.one", size)
		test.Assert(err == nil && bytes.Contains(output, data),
			"Error getting the Xattribute for symlink: %v, %s",
			err, output)

		// Try to get non-existing attribute
		_, err, output = utils.LGetXattr(symlFilename, "security.two", size)
		test.Assert(err != nil,
			"Error getting non-existing Xattribute for symlink: %v, %s",
			err, output)

		// Switch Content of the assigned attribute
		err = utils.LSetXattr(symlFilename, "security.one", data2, 0)
		test.Assert(err == nil, "Error reset XAttr: %v", err)
		_, err, output = utils.LGetXattr(symlFilename, "security.one", size)
		test.Assert(err == nil && !bytes.Contains(output, data) &&
			bytes.Contains(output, data2),
			"Error get reset XAttr: %v, %s", err, output)

		// Verify the impact on the normal attribute
		out := make([]byte, size, 2*size)
		_, err = syscall.Getxattr(symlFilename, "security.one", out)
		test.Assert(err != nil, "Error impact on the normal XAttr: %v, %s",
			err, out)
	})
}

// Verify the list/remove XAttr function for Symlink's own Extended Attributes
func TestSymlinkXAttrListRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		symlFilename := createSymlink(workspace, test)

		const size = 64

		// Add three XAttr to test the list function
		err := utils.LSetXattr(symlFilename, "security.one",
			[]byte("TestOne"), 0)
		test.Assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		err = utils.LSetXattr(symlFilename, "security.two",
			[]byte("TestTwo"), 0)
		test.Assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		err = utils.LSetXattr(symlFilename,
			"security.three", []byte("TestThree"), 0)
		test.Assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		_, err, output := utils.LListXattr(symlFilename, size)
		test.Assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing all of XAttr: %v, %s", err, output)

		// verify the removing function
		err = utils.LRemoveXattr(symlFilename, "security.two")
		test.Assert(err == nil, "Error removing XAttr: %v, %s", err, output)

		_, err, output = utils.LListXattr(symlFilename, size)
		test.Assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			!bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing rest of XAttr: %v, %s", err, output)

		// verify the attempt to remove non-existing attribute
		err = utils.LRemoveXattr(symlFilename, "security.two")
		test.Assert(err != nil, "Error removing XAttr: %v", err)

		_, err, output = utils.LListXattr(symlFilename, size)
		test.Assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			!bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing non-existing XAttr: %v, %s",
			err, output)

	})
}

func TestSymlinkPermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		symlink := createSymlink(workspace, test)

		var stat syscall.Stat_t
		err := syscall.Lstat(symlink, &stat)
		test.Assert(err == nil, "Error stat'ing test symlink: %v", err)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFLNK
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.Assert(stat.Mode == expectedPermissions,
			"Symlink has wrong permissions %x", stat.Mode)
	})
}
