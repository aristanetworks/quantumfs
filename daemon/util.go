// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"

// A number of utility functions. It'd be nice to create packages for these
// elsewhere. Maybe a 'bit' package.

// Given a bitflag field and an integer of flags, return whether the flags are set or
// not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}

// Convert the given null terminated byte array into a string
func BytesToString(data []byte) string {
	length := bytes.IndexByte(data, 0)
	if length == -1 {
		length = len(data)
	}
	return string(data[:length])
}

// Convert the given null terminated string into a [256]byte array
func StringToBytes256(data string) [256]byte {
	var out [256]byte
	in := []byte(data)
	for i := range in {
		out[i] = in[i]
	}

	return out
}

// Given an integer, return the number of blocks of the given size necessary to
// contain it.
func BlocksRoundUp(len uint64, blockSize uint64) uint64 {
	blocks := len / blockSize
	if len%blockSize != 0 {
		blocks++
	}

	return blocks
}

// Panic with the given message if the condition isn't true.
func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
