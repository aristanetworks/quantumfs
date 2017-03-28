// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "syscall"
import "unsafe"

// NOTE: syscall.Listxattr follows a symlink
//  SYSCALL_LLISTXATTR isn't available in the golang's syscall
//  library. Same for syscall.Getxattr
//  So following are custom implementations that support
//  symlink follow

var _zero uintptr

// a customized symlink XAttribute set command
func SetXattr(path string, attr string, data []byte, flags int) (err error) {
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

// a customized symlink XAttribute get command
func GetXattr(path string, attr string,
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

// a customized symlink XAttribute list command
func ListXattr(path string, size int) (sz int, err error, output []byte) {
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
func RemoveXattr(path string, attr string) (err error) {
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
