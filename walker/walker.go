// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "fmt"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"

// Walk the workspace hierarchy
func Walk(ds quantumfs.DataStore, db quantumfs.WorkspaceDB,
	rootID quantumfs.ObjectKey,
	f func(string, quantumfs.ObjectKey, uint64) error) error {
	if rootID.Type() != quantumfs.KeyTypeMetadata {
		return fmt.Errorf(
			"Type of rootID %s is %s instead of KeyTypeMetadata",
			rootID.String(), key2String(rootID))
	}

	buf := quantumfs.NewTestBuffer(nil, rootID)
	err := ds.Get(nil, rootID, buf)
	if err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s",
		key2String(rootID))
	wsr := buf.AsWorkspaceRoot()

	// TODO: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	return handleDirectoryEntry("/", ds, wsr.BaseLayer(), f)
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

func handleMultiBlockFile(path string,
	ds quantumfs.DataStore,
	key quantumfs.ObjectKey,
	f func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := quantumfs.NewTestBuffer(nil, key)
	if err := ds.Get(nil, key, buf); err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"MultiBlockFile buffer %s",
		key2String(key))

	mbf := buf.AsMultiBlockFile()
	keys := mbf.ListOfBlocks()
	for i, k := range keys {
		if i == len(keys)-1 {
			err := f(path, k, uint64(mbf.SizeOfLastBlock()))
			if err != nil {
				return err
			}
		}
		err := f(path, k, uint64(mbf.BlockSize()))
		if err != nil {
			return err
		}
	}

	return nil
}

func handleVeryLargeFile(path string,
	ds quantumfs.DataStore,
	key quantumfs.ObjectKey,
	f func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := quantumfs.NewTestBuffer(nil, key)
	err := ds.Get(nil, key, buf)
	if err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"VeryLargeFile buffer %s",
		key2String(key))

	vlf := buf.AsVeryLargeFile()
	for part := 0; part < vlf.NumberOfParts(); part++ {
		err = handleMultiBlockFile(path, ds, vlf.LargeFileKey(part), f)
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
	f func(string, quantumfs.ObjectKey, uint64) error) error {

	buf := quantumfs.NewTestBuffer(nil, key)
	err := ds.Get(nil, key, buf)
	if err != nil {
		return err
	}

	assertNonZeroBuf(buf,
		"DirectoryEntry buffer %s",
		key2String(key))

	de := buf.AsDirectoryEntry()
	for i := 0; i < de.NumEntries(); i++ {
		dr := de.Entry(i)
		totalFilesWalked++
		fpath := filepath.Join(path, dr.Filename())

		switch dr.Type() {
		case quantumfs.ObjectTypeSmallFile:
			f(fpath, dr.ID(), dr.Size())
		case quantumfs.ObjectTypeMediumFile:
			fallthrough
		case quantumfs.ObjectTypeLargeFile:
			if err = handleMultiBlockFile(fpath,
				ds, dr.ID(), f); err != nil {
				return err
			}
		case quantumfs.ObjectTypeVeryLargeFile:
			if err = handleVeryLargeFile(fpath,
				ds, dr.ID(), f); err != nil {
				return err
			}
		case quantumfs.ObjectTypeDirectoryEntry:
			if !dr.ID().IsEqualTo(quantumfs.EmptyDirKey) {
				if err = handleDirectoryEntry(fpath,
					ds, dr.ID(), f); err != nil {
					return err
				}
			}
		case quantumfs.ObjectTypeSymlink:
			// dr.ID() is KeyTypeMetadata which points to a block
			// whose content is the file path to which the symlink points
			// at. The size of the block is available in dr.Size()
			err = f(fpath, dr.ID(), dr.Size())
			if err != nil {
				return err
			}
		default:
			//fmt.Println(dr.Filename(), dr.Size(),
			//	quantumfs.ObjectType2String(dr.Type()), key2String(dr.ID()))
		}
	}

	return nil
}
