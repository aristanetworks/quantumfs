// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "sync/atomic"

import "github.com/aristanetworks/quantumfs"

func WriteWorkspaceRoot(rootDirKey quantumfs.ObjectKey,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	// publish all the hardlinks for this wsr
	hardLinkEntry, herr := writeHardLinkInfo(ds)
	if herr != nil {
		return quantumfs.ZeroKey,
			fmt.Errorf("Write hard info inf failed: %v", herr)
	}

	// write WSR
	wsr := quantumfs.NewWorkspaceRoot()
	// root's DirectoryRecord object isn't needed, only the key
	// to access root's DirectoryEntry object is sufficient
	wsr.SetBaseLayer(rootDirKey)
	wsr.SetHardlinkEntry(hardLinkEntry)
	wKey, werr := writeBlob(wsr.Bytes(), quantumfs.KeyTypeMetadata, ds)
	if werr != nil {
		return quantumfs.ZeroKey,
			fmt.Errorf("Write workspace root failed: %v", werr)
	}
	atomic.AddUint64(&MetadataBytesWritten, uint64(len(wsr.Bytes())))
	return wKey, nil
}
