// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on symlinks, only contains: CreateSymlink and
// extended attributes related operations: set, get, list, remove

import "bytes"
import "syscall"
import "testing"
import "unsafe"

var _zero uintptr

// create a source file and link it to a symlink file
func createSymlink(workspace string, test *TestHelper) string {
	targetFilename := workspace + "/target"
	fd, err := syscall.Creat(targetFilename, 0124)
	test.assert(err == nil, "Error creating file: %v", err)

	err = syscall.Close(fd)
	test.assert(err == nil, "Error closing fd: %v", err)

	linkFilename := workspace + "/symlink"
	err = syscall.Symlink(targetFilename, linkFilename)
	test.assert(err == nil, "Error creating symlink: %v", err)
	return linkFilename
}

// create a customized symlink XAttribute set command
func lSetXattr(path string, attr string, data []byte, flags int) (err error) {
	var path_str_ptr *byte
	path_str_ptr, err = syscall.BytePtrFromString(path)
	if err != nil {
		return
	}

	var attr_str_ptr *byte
	attr_str_ptr, err = syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}
	var data_buf_ptr unsafe.Pointer
	if len(data) > 0 {
		data_buf_ptr = unsafe.Pointer(&data[0])
	} else {
		data_buf_ptr = unsafe.Pointer(&_zero)
	}
	_, _, e1 := syscall.Syscall6(syscall.SYS_LSETXATTR,
		uintptr(unsafe.Pointer(path_str_ptr)),
		uintptr(unsafe.Pointer(attr_str_ptr)),
		uintptr(data_buf_ptr), uintptr(len(data)), uintptr(flags), 0)

	if e1 != 0 {
		err = e1
	}
	return
}

// create a customized symlink XAttribute get command
func lGetXattr(path string, attr string,
	size int) (sz int, err error, output []byte) {
	var path_str_ptr *byte
	path_str_ptr, err = syscall.BytePtrFromString(path)
	if err != nil {
		return
	}
	var attr_str_ptr *byte
	attr_str_ptr, err = syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}
	var dest_buf_ptr unsafe.Pointer
	dest := make([]byte, size, size*2)
	if size > 0 {
		dest_buf_ptr = unsafe.Pointer(&dest[0])
	} else {
		dest_buf_ptr = unsafe.Pointer(&_zero)
	}
	r0, _, e1 := syscall.Syscall6(syscall.SYS_LGETXATTR,
		uintptr(unsafe.Pointer(path_str_ptr)),
		uintptr(unsafe.Pointer(attr_str_ptr)),
		uintptr(dest_buf_ptr), uintptr(len(dest)), 0, 0)

	sz = int(r0)
	if e1 != 0 {
		err = e1
	}
	output = dest
	return
}

// create a customized symlink XAttribute list command
func lListXattr(path string, size int) (sz int, err error, output []byte) {
	var path_str_ptr *byte
	path_str_ptr, err = syscall.BytePtrFromString(path)
	if err != nil {
		return
	}
	var dest_buf_ptr unsafe.Pointer
	dest := make([]byte, size, size*2)
	if size > 0 {
		dest_buf_ptr = unsafe.Pointer(&dest[0])
	} else {
		dest_buf_ptr = unsafe.Pointer(&_zero)
	}
	r0, _, e1 := syscall.Syscall(syscall.SYS_LLISTXATTR,
		uintptr(unsafe.Pointer(path_str_ptr)),
		uintptr(dest_buf_ptr), uintptr(len(dest)))
	sz = int(r0)
	if e1 != 0 {
		err = e1
	}
	output = dest
	return
}

// create a customized symlink XAttribute remove command
func lRemoveXattr(path string, attr string) (err error) {
	var path_str_ptr *byte
	path_str_ptr, err = syscall.BytePtrFromString(path)
	if err != nil {
		return
	}
	var attr_str_ptr *byte
	attr_str_ptr, err = syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}
	_, _, e1 := syscall.Syscall(syscall.SYS_LREMOVEXATTR,
		uintptr(unsafe.Pointer(path_str_ptr)),
		uintptr(unsafe.Pointer(attr_str_ptr)), 0)
	if e1 != 0 {
		err = e1
	}
	return
}

// Verify the creation of symlink file
func TestSymlinkCreation(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		// create a symlink file
		linkFilename := createSymlink(workspace, test)

		// test the status of the symlink file
		var stat syscall.Stat_t
		err := syscall.Lstat(linkFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
		test.assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)
	})
}

// Verify the set/get XAttr function for Symlink's own Extended Attributes
func TestSymlinkXAttrSetGet(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		symlFilename := createSymlink(workspace, test)

		const size = 32
		data := []byte("TestOne")
		data2 := []byte("TestTwo")

		// verify the forbidden namespace "user"
		// Test the consistance b/t Unix and QuantumFS
		// The reference about namespace can be found on
		// http://man7.org/linux/man-pages/man7/xattr.7.html
		err := lSetXattr(symlFilename, "user.one", data, 0)
		test.assert(err != nil,
			"Error setting XAttr not supposed to run")

		// verify the normal namespace "security"
		err = lSetXattr(symlFilename, "security.one", data, 0)
		test.assert(err == nil, "Error setting XAttr: %v", err)

		// verify the attribute is actually stored
		_, err, output := lGetXattr(symlFilename, "security.one", size)
		test.assert(err == nil && bytes.Contains(output, data),
			"Error getting the Xattribute for symlink: %v, %s",
			err, output)

		// Try to get non-existing attribute
		_, err, output = lGetXattr(symlFilename, "security.two", size)
		test.assert(err != nil,
			"Error getting non-existing Xattribute for symlink: %v, %s",
			err, output)

		// Switch Content of the assigned attribute
		err = lSetXattr(symlFilename, "security.one", data2, 0)
		test.assert(err == nil, "Error reset XAttr: %v", err)
		_, err, output = lGetXattr(symlFilename, "security.one", size)
		test.assert(err == nil && !bytes.Contains(output, data) &&
			bytes.Contains(output, data2),
			"Error get reset XAttr: %v, %s", err, output)

		// Verify the impact on the normal attribute
		out := make([]byte, size, 2*size)
		_, err = syscall.Getxattr(symlFilename, "security.one", out)
		test.assert(err != nil, "Error impact on the normal XAttr: %v, %s",
			err, out)
	})
}

// Verify the list/remove XAttr function for Symlink's own Extended Attributes
func TestSymlinkXAttrListRemove(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		symlFilename := createSymlink(workspace, test)

		const size = 64

		// Add three XAttr to test the list function
		err := lSetXattr(symlFilename, "security.one", []byte("TestOne"), 0)
		test.assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		err = lSetXattr(symlFilename, "security.two", []byte("TestTwo"), 0)
		test.assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		err = lSetXattr(symlFilename,
			"security.three", []byte("TestThree"), 0)
		test.assert(err == nil,
			"Error setting XAttr for list function: %v", err)
		_, err, output := lListXattr(symlFilename, size)
		test.assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing all of XAttr: %v, %s", err, output)

		// verify the removing function
		err = lRemoveXattr(symlFilename, "security.two")
		test.assert(err == nil, "Error removing XAttr: %v, %s", err, output)

		_, err, output = lListXattr(symlFilename, size)
		test.assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			!bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing rest of XAttr: %v, %s", err, output)

		// verify the attempt to remove non-existing attribute
		err = lRemoveXattr(symlFilename, "security.two")
		test.assert(err != nil, "Error removing XAttr: %v", err)

		_, err, output = lListXattr(symlFilename, size)
		test.assert(err == nil &&
			bytes.Contains(output, []byte("security.one")) &&
			!bytes.Contains(output, []byte("security.two")) &&
			bytes.Contains(output, []byte("security.three")),
			"Error listing non-existing XAttr: %v, %s",
			err, output)

	})
}

func TestSymlinkPermission(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		symlink := createSymlink(workspace, test)

		var stat syscall.Stat_t
		err := syscall.Lstat(symlink, &stat)
		test.assert(err == nil, "Error stat'ing test symlink: %v", err)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFLNK
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.assert(stat.Mode == expectedPermissions,
			"Symlink has wrong permissions %x", stat.Mode)
	})
}
