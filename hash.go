// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The hashing framework
package quantumfs

import "crypto/sha1"

/*
#cgo LDFLAGS: /usr/local/lib/libcityhash.a

#include <stdint.h>
#include <stddef.h>

void cCityHash128(const char *s, size_t len, uint64_t *lower, uint64_t *upper);
void cCityHashCrc256(const char *s, size_t len, uint64_t *result);
*/
import "C"
import "unsafe"

const hashSize = sha1.Size

func Hash(input []byte) [hashSize]byte {
	return sha1.Sum(input)
}

// CityHash wrapper
func CityHash128(input []byte) [16]byte {
	var hash [16]byte	
	C.cCityHash128((*C.char)(unsafe.Pointer(&input[0])), C.size_t(len(input)),
		(*C.uint64_t)(unsafe.Pointer(&hash[0])),
		(*C.uint64_t)(unsafe.Pointer(&hash[8])))
	return hash
}

func CityHash256(input []byte) [32]byte {
	var hash [32]byte
	C.cCityHashCrc256((*C.char)(unsafe.Pointer(&input[0])), C.size_t(len(input)),
		(*C.uint64_t)(unsafe.Pointer(&hash[0])))
	return hash
}
