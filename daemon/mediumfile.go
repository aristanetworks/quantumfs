// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// This contains medium file types and methods

import "github.com/aristanetworks/quantumfs"

type MediumFile struct {
	MultiBlockFile
}

// Shell constructor
func newMediumShell() MediumFile {
	var rtn MediumFile
	initMultiBlockAccessor(&rtn.MultiBlockFile, quantumfs.MaxBlocksMediumFile())

	return rtn
}

func newMediumAccessor(c *ctx, key quantumfs.ObjectKey) *MediumFile {
	defer c.funcIn("newMediumAccessor").Out()
	var rtn MediumFile

	multiFile := newMultiBlockAccessor(c, key, quantumfs.MaxBlocksMediumFile())
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
	defer c.funcIn("MediumFile::convertTo").Out()
	if newType <= quantumfs.ObjectTypeMediumFile {
		return fi
	}

	if newType == quantumfs.ObjectTypeLargeFile ||
		newType == quantumfs.ObjectTypeVeryLargeFile {
		rtn := newLargeShell()
		rtn.metadata = fi.metadata
		rtn.toSync = fi.toSync

		if newType == quantumfs.ObjectTypeVeryLargeFile {
			return newVeryLargeShell(&rtn)
		}

		return &rtn
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}
