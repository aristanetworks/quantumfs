// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "github.com/aristanetworks/quantumfs"

func WriteWorkspaceRoot(baseDir string, records []*quantumfs.DirectoryRecord,
	hlinks []*HardLinkInfo, ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	// write rootDir with records
	rootDirRecord, err := WriteDirectory("", "", records, ds)
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}

	// write WSR
	wsr := quantumfs.NewWorkspaceRoot()
	// root's DirectoryRecord object isn't needed, only the key
	// to access root's DirectoryEntry object is sufficient
	wsr.SetBaseLayer(rootDirRecord.ID())
	//TODO(krishna): publish hardlink map
	return writeBlob(wsr.Bytes(), quantumfs.KeyTypeMetadata, ds)
}
