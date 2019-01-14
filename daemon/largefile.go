// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// This contains large file types and methods

import "github.com/aristanetworks/quantumfs"

type LargeFile struct {
	MultiBlockFile
}

// Shell constructor
func newLargeShell() LargeFile {
	var rtn LargeFile
	initMultiBlockAccessor(&rtn.MultiBlockFile, quantumfs.MaxBlocksLargeFile())

	return rtn
}

func newLargeAccessor(c *ctx, key quantumfs.ObjectKey) *LargeFile {
	defer c.funcIn("newLargeAccessor").Out()
	var rtn LargeFile

	multiFile := newMultiBlockAccessor(c, key, quantumfs.MaxBlocksLargeFile())
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
	defer c.funcIn("LargeFile::convertTo").Out()
	if newType <= quantumfs.ObjectTypeLargeFile {
		return fi
	}

	if newType == quantumfs.ObjectTypeVeryLargeFile {
		return newVeryLargeShell(fi)
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}
