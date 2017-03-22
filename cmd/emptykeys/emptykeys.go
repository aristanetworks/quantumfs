// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// A utility which computes the constant keys for the various empty block types
package main

//import "encoding/hex"
import "fmt"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"

func main() {
	computeEmptyBlock()
	emptyDir := computeEmptyDirectory()
	computeEmptyWorkspace(emptyDir)
}

func printHash(hash [hash.HashSize]byte, name string) {
	key := quantumfs.NewObjectKey(quantumfs.KeyTypeConstant, hash)
	fmt.Printf("Empty %s key: %v\n", name, key)
}

func computeEmptyBlock() {
	var bytes []byte

	hash := hash.Hash(bytes)
	printHash(hash, "block")
}

func computeEmptyDirectory() [hash.HashSize]byte {
	emptyDir := quantumfs.NewDirectoryEntry()
	data := emptyDir.Bytes()
	hash := hash.Hash(data)

	printHash(hash, "directory")
	return hash
}

func computeEmptyWorkspace(emptyDir [hash.HashSize]byte) {
	emptyDirKey := quantumfs.NewObjectKey(quantumfs.KeyTypeConstant, emptyDir)

	emptyWorkspace := quantumfs.NewWorkspaceRoot()
	emptyWorkspace.SetBaseLayer(emptyDirKey)
	emptyWorkspace.SetVcsLayer(emptyDirKey)
	emptyWorkspace.SetBuildLayer(emptyDirKey)
	emptyWorkspace.SetUserLayer(emptyDirKey)

	bytes := emptyWorkspace.Bytes()
	hash := hash.Hash(bytes)

	printHash(hash, "workspace")
}
