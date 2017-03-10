// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The hashing framework
package quantumfs

import "crypto/sha1"

const hashSize = sha1.Size

func Hash(input []byte) [hashSize]byte {
	return sha1.Sum(input)
}
