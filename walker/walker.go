// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "fmt"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"

// Walk the workspace hierarchy
func Walk(ds quantumfs.DataStore, rootID quantumfs.ObjectKey,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	if rootID.Type() != quantumfs.KeyTypeMetadata {
		return fmt.Errorf(
			"Type of rootID %s is %s instead of KeyTypeMetadata",
			rootID.String(), key2String(rootID))
	}

	buf := testutils.NewSimpleBuffer(nil, rootID)
	err := ds.Get(nil, rootID, buf)
	if err != nil {
		return err
	}
	assertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s",
		key2String(rootID))

	err = walkFunc("", rootID, uint64(buf.Size()))
	if err != nil {
		return err
	}
	wsr := buf.AsWorkspaceRoot()
	//===============================================
	// TODO: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	if err = handleHardLinks(ds, wsr.HardlinkEntry(), walkFunc); err != nil {
		return err
	}
	return handleDirectoryEntry("/", ds, wsr.BaseLayer(), walkFunc)
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

// TODO: move this to quantumfs package
func assertNonZeroBuf(buf quantumfs.Buffer,
	format string, args ...string) {

	if buf.Size() == 0 {
		panic(fmt.Sprintf(format, args))
	}
}

func handleHardLinks(ds quantumfs.DataStore,
	hle quantumfs.HardlinkEntry,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	for {
		//  Go through all records in this entry.
		for idx := 0; idx < hle.NumEntries(); idx++ {

			hlr := hle.Entry(idx)
			dr := hlr.Record()
			linkPath := dr.Filename()
			err := handleDirectoryRecord(linkPath, ds, dr, walkFunc)
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
		err := ds.Get(nil, key, buf)
		if err != nil {
			return err
		}

		assertNonZeroBuf(buf,
			"WorkspaceRoot buffer %s",
			key2String(key))

		err = walkFunc("", key, uint64(buf.Size()))
		if err != nil {
			return err
		}

		hle = buf.AsHardlinkEntry()
	}
	return nil

}

func handleMultiBlockFile(path string,
	ds quantumfs.DataStore,
	key quantumfs.ObjectKey,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	if err := ds.Get(nil, key, buf); err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"MultiBlockFile buffer %s",
		key2String(key))

	err := walkFunc(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	mbf := buf.AsMultiBlockFile()
	keys := mbf.ListOfBlocks()
	for i, k := range keys {
		if i == len(keys)-1 {
			err := walkFunc(path, k, uint64(mbf.SizeOfLastBlock()))
			if err != nil {
				return err
			}
		}
		err := walkFunc(path, k, uint64(mbf.BlockSize()))
		if err != nil {
			return err
		}
	}

	return nil
}

func handleVeryLargeFile(path string,
	ds quantumfs.DataStore,
	key quantumfs.ObjectKey,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	err := ds.Get(nil, key, buf)
	if err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"VeryLargeFile buffer %s",
		key2String(key))

	err = walkFunc(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	vlf := buf.AsVeryLargeFile()
	for part := 0; part < vlf.NumberOfParts(); part++ {
		err = handleMultiBlockFile(path, ds, vlf.LargeFileKey(part),
			walkFunc)
		if err != nil {
			return err
		}
	}

	return nil
}

var totalFilesWalked uint64

func handleDirectoryEntry(path string,
	ds quantumfs.DataStore,
	key quantumfs.ObjectKey,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	err := ds.Get(nil, key, buf)
	if err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"DirectoryEntry buffer %s",
		key2String(key))

	err = walkFunc(path, key, uint64(buf.Size()))
	if err != nil {
		return err
	}

	de := buf.AsDirectoryEntry()
	for i := 0; i < de.NumEntries(); i++ {
		err = handleDirectoryRecord(path, ds, de.Entry(i), walkFunc)
		if err != nil {
			return err
		}
	}

	return nil
}

func handleDirectoryRecord(path string,
	ds quantumfs.DataStore,
	dr *quantumfs.DirectRecord,
	walkFunc func(string, quantumfs.ObjectKey, uint64) error) error {

	fpath := filepath.Join(path, dr.Filename())

	switch dr.Type() {
	case quantumfs.ObjectTypeSmallFile:
		walkFunc(fpath, dr.ID(), dr.Size())
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		if err := handleMultiBlockFile(fpath,
			ds, dr.ID(), walkFunc); err != nil {
			return err
		}
	case quantumfs.ObjectTypeVeryLargeFile:
		if err := handleVeryLargeFile(fpath,
			ds, dr.ID(), walkFunc); err != nil {
			return err
		}
	case quantumfs.ObjectTypeSymlink:
		// dr.ID() is KeyTypeMetadata which points to a block
		// whose content is the file path to which the symlink points
		// at. The size of the block is available in dr.Size()
		err := walkFunc(fpath, dr.ID(), dr.Size())
		if err != nil {
			return err
		}
	case quantumfs.ObjectTypeDirectoryEntry:
		if !dr.ID().IsEqualTo(quantumfs.EmptyDirKey) {
			if err := handleDirectoryEntry(fpath,
				ds, dr.ID(), walkFunc); err != nil {
				return err
			}
		}
	default:
	}

	return nil
}
