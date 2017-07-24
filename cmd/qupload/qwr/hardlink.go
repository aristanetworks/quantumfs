// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"os"
	"sync/atomic"
	"syscall"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type HardLinkInfo struct {
	record *quantumfs.DirectRecord
	nlinks uint32
}

// stat.Ino -> hardLinkInfo mapping
// assumes that the inodes being checked
// belong to the same filesystem
var hardLinkInfoMap = make(map[uint64]*HardLinkInfo)

// needed for a concurrent client of qwr
var hardLinkInfoMutex utils.DeferableMutex

func HardLink(finfo os.FileInfo) (quantumfs.DirectoryRecord, bool) {
	defer hardLinkInfoMutex.Lock().Unlock()

	stat := finfo.Sys().(*syscall.Stat_t)
	hlinfo, exists := hardLinkInfoMap[stat.Ino]
	if !exists {

		// the FileInfo.Stat already indicates the
		// final link count for path but we start with 1
		// since its possible that a writer selects only
		// directories for upload and so the link count
		// should reflect the entries written to QFS
		// datastore
		hlinfo := &HardLinkInfo{
			record: nil,
			nlinks: 1,
		}
		// actual record is stored in SetHardLink
		// its ok until then since all other callers
		// don't need that information
		hardLinkInfoMap[stat.Ino] = hlinfo
		return nil, false
	}

	hlinfo.nlinks++
	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetFilename(finfo.Name())

	return newDirRecord, true
}

func SetHardLink(finfo os.FileInfo,
	record *quantumfs.DirectRecord) quantumfs.DirectoryRecord {

	// need locks to protect the map
	defer hardLinkInfoMutex.Lock().Unlock()

	// only one caller will do a SetHardLink
	stat := finfo.Sys().(*syscall.Stat_t)
	hlinfo := hardLinkInfoMap[stat.Ino]

	hlinfo.record = record

	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetFilename(record.Filename())

	return newDirRecord
}

func writeHardLinkInfo(qctx *quantumfs.Ctx,
	ds quantumfs.DataStore) (*quantumfs.HardlinkEntry, error) {

	// entryIdx indexes into the metadata block
	entryNum := len(hardLinkInfoMap)
	entryNum, hle := quantumfs.NewHardlinkEntry(entryNum)
	hleKey := quantumfs.EmptyDirKey
	entryIdx := 0
	var err error
	for _, hlinfo := range hardLinkInfoMap {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			hle.SetNumEntries(entryIdx)
			hle.SetNext(hleKey)

			hleKey, err = writeBlock(qctx, hle.Bytes(),
				quantumfs.KeyTypeMetadata, ds)
			if err != nil {
				return nil, err
			}
			atomic.AddUint64(&MetadataBytesWritten,
				uint64(len(hle.Bytes())))

			entryNum, hle = quantumfs.NewHardlinkEntry(entryNum)
			entryIdx = 0
		}

		hlr := quantumfs.NewHardlinkRecord()
		hlr.SetRecord(hlinfo.record)
		hlr.SetNlinks(hlinfo.nlinks)

		hle.SetEntry(entryIdx, hlr)
		entryIdx++
	}

	hle.SetNumEntries(entryIdx)
	hle.SetNext(hleKey)

	// last HardLinkEntry is embedded into
	// workspace root
	return hle, nil
}
