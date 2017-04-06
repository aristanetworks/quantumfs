// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "fmt"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"

type walkFunc func(string, quantumfs.ObjectKey, uint64) error

// Walk the workspace hierarchy
func Walk(c *quantumfs.Ctx, ds quantumfs.DataStore, rootID quantumfs.ObjectKey,
	wf walkFunc) error {

	if rootID.Type() != quantumfs.KeyTypeMetadata {
		return fmt.Errorf(
			"Type of rootID %s is %s instead of KeyTypeMetadata",
			rootID.String(), key2String(rootID))
	}

	buf := testutils.NewSimpleBuffer(nil, rootID)
	err := ds.Get(c, rootID, buf)
	if err != nil {
		return err
	}
	testutils.AssertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s",
		key2String(rootID))

	err = wf("", rootID, uint64(buf.Size()))
	if err != nil {
		return err
	}
	wsr := buf.AsWorkspaceRoot()
	//===============================================
	// NOTE: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	if err = handleHardLinks(c, ds, wsr.HardlinkEntry(), wf); err != nil {
		return err
	}
	return handleDirectoryEntry(c, "/", ds, wsr.BaseLayer(), wf)
}

func key2String(key quantumfs.ObjectKey) string {
	switch {

	case key.IsEqualTo(quantumfs.EmptyDirKey):
		return "EmptyDirKey"
	case key.IsEqualTo(quantumfs.EmptyBlockKey):
		return "EmptyBlockKey"
	case key.IsEqualTo(quantumfs.EmptyWorkspaceKey):
		return "EmptyWorkspaceKey"
	case key.IsEqualTo(quantumfs.ZeroKey):
		return "ZeroKey"
	default:
		return key.String()
	}
}

func handleHardLinks(c *quantumfs.Ctx, ds quantumfs.DataStore,
	hle quantumfs.HardlinkEntry, wf walkFunc) error {

	for {
		//  Go through all records in this entry.
		for idx := 0; idx < hle.NumEntries(); idx++ {

			hlr := hle.Entry(idx)
			dr := hlr.Record()
			linkPath := dr.Filename()
			err := handleDirectoryRecord(c, linkPath, ds, dr, wf)
			if err != nil {
				return err
			}
		}
		// Go to next Entry
		if hle.Next().IsEqualTo(quantumfs.EmptyDirKey) ||
			hle.NumEntries() == 0 {
			break
		}

		key := hle.Next()
		buf := testutils.NewSimpleBuffer(nil, key)
		err := ds.Get(c, key, buf)
		if err != nil {
			return err
		}

		testutils.AssertNonZeroBuf(buf,
			"WorkspaceRoot buffer %s",
			key2String(key))

		err = wf("", key, uint64(buf.Size()))
		if err != nil {
			return err
		}

		hle = buf.AsHardlinkEntry()
	}
	return nil

}

func handleMultiBlockFile(c *quantumfs.Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc) error {
	buf := testutils.NewSimpleBuffer(nil, key)
	if err := ds.Get(c, key, buf); err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"MultiBlockFile buffer %s",
		key2String(key))

	err := wf(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	mbf := buf.AsMultiBlockFile()
	keys := mbf.ListOfBlocks()
	for i, k := range keys {
		if i == len(keys)-1 {
			err := wf(path, k, uint64(mbf.SizeOfLastBlock()))
			return err // Return, since this is last block
		}
		err := wf(path, k, uint64(mbf.BlockSize()))
		if err != nil {
			return err
		}
	}

	return nil
}

func handleVeryLargeFile(c *quantumfs.Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	err := ds.Get(c, key, buf)
	if err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"VeryLargeFile buffer %s",
		key2String(key))

	err = wf(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	vlf := buf.AsVeryLargeFile()
	for part := 0; part < vlf.NumberOfParts(); part++ {
		err = handleMultiBlockFile(c, path, ds,
			vlf.LargeFileKey(part), wf)
		if err != nil {
			return err
		}
	}

	return nil
}

var totalFilesWalked uint64

func handleDirectoryEntry(c *quantumfs.Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	err := ds.Get(c, key, buf)
	if err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"DirectoryEntry buffer %s",
		key2String(key))

	err = wf(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	de := buf.AsDirectoryEntry()
	for i := 0; i < de.NumEntries(); i++ {
		err = handleDirectoryRecord(c, path, ds, de.Entry(i), wf)
		if err != nil {
			return err
		}
	}

	return nil
}

func handleDirectoryRecord(c *quantumfs.Ctx, path string, ds quantumfs.DataStore,
	dr *quantumfs.DirectRecord, wf walkFunc) error {

	fpath := filepath.Join(path, dr.Filename())

	switch dr.Type() {
	case quantumfs.ObjectTypeSmallFile:
		wf(fpath, dr.ID(), dr.Size())
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		if err := handleMultiBlockFile(c, fpath,
			ds, dr.ID(), wf); err != nil {
			return err
		}
	case quantumfs.ObjectTypeVeryLargeFile:
		if err := handleVeryLargeFile(c, fpath,
			ds, dr.ID(), wf); err != nil {
			return err
		}
	case quantumfs.ObjectTypeSymlink:
		// dr.ID() is KeyTypeMetadata which points to a block
		// whose content is the file path to which the symlink points
		// at. The size of the block is available in dr.Size()
		err := wf(fpath, dr.ID(), dr.Size())
		if err != nil {
			return err
		}
	case quantumfs.ObjectTypeDirectoryEntry:
		if !dr.ID().IsEqualTo(quantumfs.EmptyDirKey) {
			if err := handleDirectoryEntry(c, fpath,
				ds, dr.ID(), wf); err != nil {
				return err
			}
		}
	default:
	}

	return nil
}
