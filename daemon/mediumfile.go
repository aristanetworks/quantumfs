// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains medium file types and methods

import "arista.com/quantumfs"

type MediumFile struct {
	MultiBlockFile
}

// Shell constructor
func newMediumShell() MediumFile {
	var rtn MediumFile
	rtn.maxBlocks = quantumfs.MaxBlocksMediumFile

	return rtn
}

func newMediumAccessor(c *ctx, key quantumfs.ObjectKey) *MediumFile {
	var rtn MediumFile

	multiFile := newMultiBlockAccessor(c, key, quantumfs.MaxBlocksMediumFile)
	if multiFile == nil {
		return nil
	}

	rtn.MultiBlockFile = *multiFile

	return &rtn
}

func (fi *MediumFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeMediumFile
}

func (fi *MediumFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	if newType <= quantumfs.ObjectTypeMediumFile {
		return fi
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}
