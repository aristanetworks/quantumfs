// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "bytes"
import "fmt"
import "os"
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

// MkdirAll creates a directory named path, along with any necessary parents, and
// returns nil, or else returns an error.
// The permission bits perm are used for all directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing and returns nil.
// Note: this is copied from golang source, and modified to use syscall.Mkdir because
// there's a bug with os.Mkdir where sticky bits are lost
func MkdirAll(path string, perm os.FileMode) error {
	// Fast path: if we can tell whether path is a directory or file, stop with
	// success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{"mkdir", path, syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent
		err = MkdirAll(path[0:j-1], perm)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = syscall.Mkdir(path, uint32(perm))
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}
