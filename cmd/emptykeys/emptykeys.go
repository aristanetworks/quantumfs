// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// A utility which computes the constant keys for the various empty block types
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/aristanetworks/quantumfs/utils/keycompute"
)

func main() {
	printHash(keycompute.ComputeEmptyBlock(), "block")
	printHash(keycompute.ComputeEmptyDirectory(), "directory")
	printHash(keycompute.ComputeEmptyWorkspace(emptyDir), "workspace")
}

func printHash(hash [hash.HashSize]byte, name string) {
	fmt.Printf("Empty %s hash: %s\n", name, hex.EncodeToString(hash[:]))
}
