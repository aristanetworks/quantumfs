// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "encoding/binary"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

type HardLinkID uint64

type HardLinkInfo struct {
	record *quantumfs.DirectoryRecord
	id     HardLinkID
	nlinks uint32
}

// stat.Ino -> hardLinkInfo mapping
// assumes that the inodes being checked
// belong to the same filesystem
var hardLinkInfoMap = make(map[uint64]*HardLinkInfo)
var hardLinkInfoNextID HardLinkID

func encodeHardLinkID(id HardLinkID) quantumfs.ObjectKey {
	var hash [quantumfs.ObjectKeyLength - 1]byte

	binary.LittleEndian.PutUint64(hash[0:8], uint64(id))
	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, hash)
}

func HardLinkExists(finfo os.FileInfo) (*quantumfs.DirectoryRecord, bool) {

	stat := finfo.Sys().(*syscall.Stat_t)
	hlinfo, exists := hardLinkInfoMap[stat.Ino]
	if !exists {
		return nil, false
	}

	hlinfo.nlinks++
	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetID(encodeHardLinkID(hlinfo.id))
	newDirRecord.SetFilename(finfo.Name())

	return newDirRecord, true
}

func SetHardLink(record *quantumfs.DirectoryRecord) *quantumfs.DirectoryRecord {

	hardLinkInfoNextID++

	hlinfo := &HardLinkInfo{
		record: record,
		id:     HardLinkID(hardLinkInfoNextID),
		nlinks: 1,
	}

	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetID(encodeHardLinkID(hlinfo.id))
	newDirRecord.SetFilename(record.Filename())

	return newDirRecord
}
