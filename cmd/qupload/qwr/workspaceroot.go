// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
)

func WriteWorkspaceRoot(qctx *quantumfs.Ctx, rootDirKey quantumfs.ObjectKey,
	ds quantumfs.DataStore, hl *Hardlinks) (rtn quantumfs.ObjectKey,
	bytesWritten uint64, err error) {

	// publish all the hardlinks for this wsr
	hardLinkEntry, totalWritten, herr := hl.writeHardLinkInfo(qctx, ds)
	if herr != nil {
		return quantumfs.ZeroKey, 0,
			fmt.Errorf("Write hard info inf failed: %v", herr)
	}

	// write WSR
	wsr := quantumfs.NewWorkspaceRoot()
	// root's DirectoryRecord object isn't needed, only the key
	// to access root's DirectoryEntry object is sufficient
	wsr.SetBaseLayer(rootDirKey)
	wsr.SetHardlinkEntry(hardLinkEntry)
	wKey, werr := writeBlock(qctx, wsr.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if werr != nil {
		return quantumfs.ZeroKey, 0,
			fmt.Errorf("Write workspace root failed: %v", werr)
	}

	totalWritten += uint64(len(wsr.Bytes()))
	return wKey, totalWritten, nil
}
