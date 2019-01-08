// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// A utility which computes the constant keys for the various empty block types
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/aristanetworks/quantumfs/hash"
	"github.com/aristanetworks/quantumfs/utils/keycompute"
)

func main() {
	printHash(keycompute.ComputeEmptyBlock(), "block")
	printHash(keycompute.ComputeEmptyDirectory(), "directory")
	printHash(keycompute.ComputeEmptyWorkspace(), "workspace")
}

func printHash(hash [hash.HashSize]byte, name string) {
	fmt.Printf("Empty %s hash: %s\n", name, hex.EncodeToString(hash[:]))
}
