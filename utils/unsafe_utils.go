// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"unsafe"
)

// https://segment.com/blog/allocation-efficiency-in-high-performance-go-services
// This method is copied from runtime.noescape
// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

// This method is borrowed from runtime.slicebytetostringtmp and reinterpret-casts
// a byte slice to a string and disassociates the two.

// Special care is required to handle the return value of this function as it
// will be allocated on the stack and leaking it to heap will result in a runtime
// error.
func MoveByteSliceToString(b []byte) string {
	return *(*string)(noescape(unsafe.Pointer(&b)))
}
