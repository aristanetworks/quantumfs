// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on symlinks, only contains: CreateSymlink and 
// extended attributes related operations: set, get, list, remove
// TODO: Add test functions for other symlink functions

import "bytes"
import "syscall"
import "testing"
import "unsafe"


var _zero uintptr

// create a source file and link it to a symlink file
func createSymlink(workspace string, test *testHelper) string {
        targetFilename := workspace + "/target"
        fd, err := syscall.Creat(targetFilename, 0124)
        test.assert(err == nil, "Error creating file: %v", err)

        err = syscall.Close(fd)
        test.assert(err == nil, "Error closing fd: %v", err)

        linkFilename := workspace + "/symlink"
        err = syscall.Symlink( targetFilename, linkFilename)
        test.assert(err == nil, "Error creating symlink: %v", err)
        return linkFilename
}

// create a customized symlink XAttribute set command
func setXAttr(path string, attr string, data []byte, flags int) (err error) {
        var _p0 *byte
        _p0, err = syscall.BytePtrFromString(path)
        if err != nil {
                return
        }
        
        var _p1 *byte
        _p1, err = syscall.BytePtrFromString(attr)
        if err != nil {
                return
        }
        var _p2 unsafe.Pointer
        if len(data) > 0 {
                _p2 = unsafe.Pointer(&data[0])
        } else {
                _p2 = unsafe.Pointer(&_zero)
        }
        _, _, e1 := syscall.Syscall6(syscall.SYS_LSETXATTR, 
                                        uintptr(unsafe.Pointer(_p0)),
                                        uintptr(unsafe.Pointer(_p1)), uintptr(_p2),
                                        uintptr(len(data)), uintptr(flags), 0)
        
        if e1 != 0 {
                err = e1
        }
        return   
}

// create a customized symlink XAttribute get command
func getXAttr(path string, attr string, dest []byte) (sz int, err error) {
        var _p0 *byte
        _p0, err = syscall.BytePtrFromString(path)
        if err != nil {
                return
        }
        var _p1 *byte
        _p1, err = syscall.BytePtrFromString(attr)
        if err != nil {
                return
        }
        var _p2 unsafe.Pointer
        if len(dest) > 0 {
                _p2 = unsafe.Pointer(&dest[0])
        } else {
                _p2 = unsafe.Pointer(&_zero)
        }
        r0, _, e1 := syscall.Syscall6(syscall.SYS_LGETXATTR, 
                                        uintptr(unsafe.Pointer(_p0)), 
                                        uintptr(unsafe.Pointer(_p1)),
                                        uintptr(_p2), uintptr(len(dest)), 0, 0)
        
        sz = int(r0)
        if e1 != 0 {
                err = e1
        }
        return
}

// create a customized symlink XAttribute list command
func listXAttr(path string, dest []byte) (sz int, err error) {
        var _p0 *byte
        _p0, err = syscall.BytePtrFromString(path)
        if err != nil {
                return
        }
        var _p1 unsafe.Pointer
        if len(dest) > 0 {
                _p1 = unsafe.Pointer(&dest[0])
        } else {
                _p1 = unsafe.Pointer(&_zero)
        }
        r0, _, e1 := syscall.Syscall(syscall.SYS_LLISTXATTR, 
                                        uintptr(unsafe.Pointer(_p0)),
                                        uintptr(_p1), uintptr(len(dest)))
        sz = int(r0)
        if e1 != 0 {
              err = e1
        }
        return
}

// create a customized symlink XAttribute remove command
func removeXAttr(path string, attr string) (err error) {
         var _p0 *byte
         _p0, err = syscall.BytePtrFromString(path)
         if err != nil {
                return
         }
         var _p1 *byte
         _p1, err = syscall.BytePtrFromString(attr)
         if err != nil {
              return
         }
        _, _, e1 := syscall.Syscall(syscall.SYS_LREMOVEXATTR,
                                        uintptr(unsafe.Pointer(_p0)), 
                                        uintptr(unsafe.Pointer(_p1)), 0)
        if e1 != 0 {
                err = e1
        }
        return
}

func clearArray(dest []byte, size int) {
        for i := 0; i < size; i++ {
                dest[i] = 0
        }
        return
}

// Verify the creation of symlink file
func TestSymlinkCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
               
                // create a symlink file
                linkFilename := createSymlink(workspace, test)

		// test the status of the symlink file
		var stat syscall.Stat_t
		err := syscall.Lstat(linkFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
                test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
                test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink) 
	})
}


// Verify the set/get XAttr function for Symlink's own Extended Attributes
func TestSymlinkAtrr_Set_Get(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

	       workspace := test.nullWorkspace()
               
               //workspace := "/home/yang/symTest/space1/_null/null"
                symlFilename := createSymlink(workspace, test)

		var space [32]byte
                output := space[:]  
        	data := []byte("TestOne")
		data2 := []byte("TestTwo")
                
                // verify the forbiddn namespace "user"
                err := setXAttr(symlFilename, "user.one", data, 0)
		test.assert(err != nil, "Error setXAttr not supposed to run")

                // verify the normal namespace "security"
		err = setXAttr(symlFilename, "security.one", data, 0)
                test.assert(err == nil, "Error setXAttr: %v", err)
                
                // verify the attribute is actually stored
		size, err := getXAttr(symlFilename, "security.one", output)
                test.assert(err == nil && bytes.Contains(output, data),
	                "Error getting the Xatrribute for symlink: %v, %s",
                        err, output)
                clearArray(output, size)

		// Try to get non-exist attribute
                size, err = getXAttr(symlFilename, "security.two", output)
                test.assert(err != nil && !bytes.Contains(output, data), 
                        "Error getting No-existing Xattribute for symlink: %v, %s",
                        err, output)
                clearArray(output, size)

                // Switch Content of the assigned attribute
                err = setXAttr(symlFilename, "security.one", data2, 0)
                test.assert(err == nil, "Error reset XAttr: %v", err)
                size, err = getXAttr(symlFilename, "security.one", output)
                test.assert(err == nil && !bytes.Contains(output, data) && 
                        bytes.Contains(output, data2), "Error get reset XAttr: %v, %s",
                        err, output)
                clearArray(output, size)

                // Verify the impact on the normal attribute
                size, err = syscall.Getxattr(symlFilename, "security.one", output)
                test.assert(err != nil, "Error impact on the normal XAttr: %v, %s", 
                err, output)
        })
}

// Verify the list/remove XAttr function for Symlink's own Extended Attributes
func TestSymlinkAtrr_List_Remove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
                
                workspace := test.nullWorkspace()
                symlFilename := createSymlink(workspace, test)
                
                var space [128]byte
                output := space[:]  

                // Add three XAttr to test the list function
                err := setXAttr(symlFilename, "security.one", []byte("TestOne"), 0)
                test.assert(err == nil, 
                        "Error setXAttr for list function: %v", err)
                err = setXAttr(symlFilename, "security.two", []byte("TestTwo"), 0)
                test.assert(err == nil, 
                        "Error setXAttr for list function: %v", err)
                err = setXAttr(symlFilename, "security.three", []byte("TestThree"), 0)
                test.assert(err == nil, 
                        "Error setXAttr for list function: %v", err)
                size, err := listXAttr(symlFilename, output)
                test.assert(err == nil && 
                                bytes.Contains(output, []byte("security.one")) &&
                                bytes.Contains(output, []byte("security.two")) && 
                                bytes.Contains(output, []byte("security.three")), 
                                "Error listing all of XAttr: %v, %s", err, output)
                clearArray(output, size)
                
                // verify the remving function
                err = removeXAttr(symlFilename, "security.two")
                test.assert(err == nil, "Error removing XAttr: %v, %s", err, output)
                
               
                size, err = listXAttr(symlFilename, output)    
                test.assert(err == nil && 
                                bytes.Contains(output, []byte("security.one")) && 
                                !bytes.Contains(output, []byte("security.two")) && 
                                bytes.Contains(output, []byte("security.three")),
                                "Error listing rest of XAttr: %v, %s", err, output)
                clearArray(output, size)
 
                // verify the attempt to remove no-existing attribute
                err = removeXAttr(symlFilename, "security.two")
                test.assert(err != nil, "Error removing XAttr: %v", err)
                
                size, err = listXAttr(symlFilename, output)
                test.assert(err == nil  && 
                                bytes.Contains(output, []byte("security.one")) && 
                                !bytes.Contains(output, []byte("security.two")) && 
                                bytes.Contains(output, []byte("security.three")),
                                "Error listing no-existing XAttr: %v, %s", err, output)

        })
}
