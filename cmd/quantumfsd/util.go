// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

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
