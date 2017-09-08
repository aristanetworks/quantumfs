// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package keycompute

// This file contains functions for computing empty keys for use in testing
// to ensure that emptykeys matches quantumfs

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/hash"
)

func ComputeEmptyBlock() [hash.HashSize]byte {
	var bytes []byte

	return hash.Hash(bytes)
}

func ComputeEmptyDirectory() [hash.HashSize]byte {
	_, emptyDir := quantumfs.NewDirectoryEntry(quantumfs.MaxDirectoryRecords())
	data := emptyDir.Bytes()
	return hash.Hash(data)
}

func ComputeEmptyWorkspace() [hash.HashSize]byte {
	emptyDir := ComputeEmptyDirectory()
	emptyDirKey := quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata, emptyDir)

	emptyWorkspace := quantumfs.NewWorkspaceRoot()
	emptyWorkspace.SetBaseLayer(emptyDirKey)
	emptyWorkspace.SetVcsLayer(emptyDirKey)
	emptyWorkspace.SetBuildLayer(emptyDirKey)
	emptyWorkspace.SetUserLayer(emptyDirKey)

	bytes := emptyWorkspace.Bytes()
	return hash.Hash(bytes)
}
