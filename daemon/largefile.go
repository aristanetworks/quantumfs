// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains large file types and methods

import "github.com/aristanetworks/quantumfs"

type LargeFile struct {
	MultiBlockFile
}

// Shell constructor
func newLargeShell() LargeFile {
	var rtn LargeFile
	rtn.maxBlocks = quantumfs.MaxBlocksLargeFile

	return rtn
}

func newLargeAccessor(c *ctx, key quantumfs.ObjectKey) *LargeFile {
	var rtn LargeFile

	multiFile := newMultiBlockAccessor(c, key, quantumfs.MaxBlocksLargeFile)
	if multiFile == nil {
		return nil
	}

	rtn.MultiBlockFile = *multiFile

	return &rtn
}

func (fi *LargeFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeLargeFile
}

func (fi *LargeFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	if newType <= quantumfs.ObjectTypeLargeFile {
		return fi
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *LargeFile) setFile(file *File) {
}
