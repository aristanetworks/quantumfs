// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "bytes"
import "fmt"
import "syscall"

// FileSize returns the size of a file
func FileSize(filename string) (int64, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(filename, &stat)
	if err != nil {
		return -1, fmt.Errorf("error in stating file %s:%s", filename, err)
	}
	return stat.Size, nil
}

// A number of utility functions. It'd be nice to create packages for these
// elsewhere. Maybe a 'bit' package.

// BitFlagsSet for a given a bitflag field and an integer of flags,
// returns whether the flags are set or not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}

// BitAnyFlagSet for a given a bitflag field and an integer of flags,
// returns whether any flag is set or not as a boolean.
func BitAnyFlagSet(field uint, flags uint) bool {
	if field&flags != 0 {
		return true
	}
	return false
}

// BytesToString converts the given null terminated byte array into a string.
func BytesToString(data []byte) string {
	length := bytes.IndexByte(data, 0)
	if length == -1 {
		length = len(data)
	}
	return string(data[:length])
}

// StringToBytes256 converts the given null terminated string into a
// [256]byte array.
func StringToBytes256(data string) [256]byte {
	var out [256]byte
	in := []byte(data)
	for i := range in {
		out[i] = in[i]
	}

	return out
}

// BlocksRoundUp for a given an integer, return the number of blocks of
// the given size necessary to contain it.
func BlocksRoundUp(len uint64, blockSize uint64) uint64 {
	blocks := len / blockSize
	if len%blockSize != 0 {
		blocks++
	}
	return blocks
}

// Assert the condition is true. If it is not true then fail the test with the given
// message.
func Assert(condition bool, format string, args ...interface{}) {
	if !condition {
		msg := fmt.Sprintf(format, args...)
		panic(msg)
	}
}
