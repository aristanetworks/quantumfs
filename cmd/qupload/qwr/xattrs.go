// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "strings"
import "syscall"
import "sync/atomic"
import "unsafe"

import "github.com/aristanetworks/quantumfs"

// NOTE: syscall.Listxattr follows a symlink
//  SYSCALL_LLISTXATTR isn't available in the golang's syscall
//  library. Same for syscall.Getxattr
//  So following are custom implementations that support
//  symlink follow

var _zero uintptr

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

func WriteXAttrs(path string,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	sizeofXAttrs, err, _ := lListXattr(path, 0)
	if err != nil {
		return quantumfs.EmptyBlockKey, err
	}

	if sizeofXAttrs == 0 {
		return quantumfs.EmptyBlockKey, nil
	}

	var xattrs []byte
	_, err, xattrs = lListXattr(path, sizeofXAttrs)
	if err != nil {
		return quantumfs.EmptyBlockKey, err
	}

	// attribute names are separated by null byte
	xattrNames := strings.Split(strings.Trim(string(xattrs), "\000"), "\000")

	if len(xattrNames) > quantumfs.MaxNumExtendedAttributes() {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Max number of xattrs supported is %d, found %d on"+
				" path %s\n", quantumfs.MaxNumExtendedAttributes(), len(xattrNames),
				path)
	}

	xattrMetadata := quantumfs.NewExtendedAttributes()
	for i, xattrName := range xattrNames {
		xattrSz, err, _ := lGetXattr(path, xattrName, 0)
		if err != nil {
			return quantumfs.EmptyBlockKey, err
		}
		var xattrData []byte
		_, err, xattrData = lGetXattr(path, xattrName, xattrSz)
		if err != nil {
			return quantumfs.EmptyBlockKey, err
		}

		dataKey, bErr := writeBlob(xattrData, quantumfs.KeyTypeData, ds)
		if bErr != nil {
			return quantumfs.EmptyBlockKey, bErr
		}
		atomic.AddUint64(&MetadataBytesWritten, uint64(len(xattrData)))

		xattrMetadata.SetAttribute(i, xattrName, dataKey)
		xattrMetadata.SetNumAttributes(i + 1)
	}

	xKey, xerr := writeBlob(xattrMetadata.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if xerr != nil {
		return quantumfs.EmptyBlockKey, xerr
	}
	atomic.AddUint64(&MetadataBytesWritten, uint64(len(xattrMetadata.Bytes())))
	return xKey, nil
}
