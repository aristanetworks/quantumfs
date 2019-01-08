// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// package name: libqfs
package main

import "C"

import (
	"github.com/aristanetworks/quantumfs/libqfs"
)

//export FindApiPath
func FindApiPath() (*C.char, *C.char) {
	errStr := ""
	rtn, err := libqfs.FindApiPath()
	if err != nil {
		errStr = err.Error()
	}

	return C.CString(rtn), C.CString(errStr)
}

func main() {
	// Main function must exist for this to be compiled as a shared library
}
