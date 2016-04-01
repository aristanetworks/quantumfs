// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

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
	len := bytes.IndexByte(data, 0)
	return string(data[:len])
}

// Given an integer, return the number of blocks of the given size necessary to
// contain it.
func BlocksRoundUp(len int, blockSize int) int {
	blocks := len / blockSize
	if len%blockSize != 0 {
		blocks++
	}

	return blocks
}
