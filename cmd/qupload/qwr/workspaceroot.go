// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"

import "github.com/aristanetworks/quantumfs"

func WriteWorkspaceRoot(baseDir string, records []*quantumfs.DirectoryRecord,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	fmt.Println("Writing WSR")
	// write rootDir with records
	rootDirRecord, err := WriteDirectory("", "", records, ds)
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}

	// TODO(krishna): currently xattrs cannot be saved in workspace root
	//  directory

	// publish all the hardlinks for this wsr
	hardLinkEntry, herr := writeHardLinkInfo(ds)
	if herr != nil {
		return quantumfs.ObjectKey{}, err
	}

	// write WSR
	wsr := quantumfs.NewWorkspaceRoot()
	// root's DirectoryRecord object isn't needed, only the key
	// to access root's DirectoryEntry object is sufficient
	wsr.SetBaseLayer(rootDirRecord.ID())
	wsr.SetHardlinkEntry(hardLinkEntry)
	fmt.Println("Writing WSR blob")
	return writeBlob(wsr.Bytes(), quantumfs.KeyTypeMetadata, ds)
}
