// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

// Given a bitflag field and an integer of flags, return whether the flags are set or
// not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}
