// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains large file types and methods

import "arista.com/quantumfs"

// LargeFile is basically identical to mediumFile, except that it has a couple mods
type LargeFile struct {
	MultiBlockFile
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
