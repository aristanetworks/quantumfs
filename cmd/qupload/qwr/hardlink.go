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

type Hardlinks struct {
	// stat.Ino -> hardLinkInfo mapping
	// assumes that the inodes being checked
	// belong to the same filesystem
	hardLinkInfoMap   map[uint64]*HardLinkInfo
	hardLinkInfoMutex utils.DeferableMutex
}

func NewHardlinks() *Hardlinks {
	return &Hardlinks{
		hardLinkInfoMap: make(map[uint64]*HardLinkInfo),
	}
}

// hardlinkInfoMutex must be held
func (hl *Hardlinks) getHardlink_(finfo os.FileInfo) quantumfs.DirectoryRecord {
	stat := finfo.Sys().(*syscall.Stat_t)
	hlinfo, exists := hl.hardLinkInfoMap[stat.Ino]
	if !exists {
		return nil
	}

	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetFilename(finfo.Name())
	rec := hlinfo.record
	if rec == nil {
		panic("Nil record should never happen for hardlink")
	}
	newDirRecord.SetFileId(rec.FileId())

	return newDirRecord
}

// This function is to increment an existing hardlink only
func (hl *Hardlinks) IncrementHardLink(finfo os.FileInfo) (incSuccess bool,
	record quantumfs.DirectoryRecord) {

	defer hl.hardLinkInfoMutex.Lock().Unlock()

	stat := finfo.Sys().(*syscall.Stat_t)
	hlrec := hl.getHardlink_(finfo)
	if hlrec == nil {
		return false, nil
	}

	hlinfo := hl.hardLinkInfoMap[stat.Ino]
	hlinfo.nlinks++
	hl.hardLinkInfoMap[stat.Ino] = hlinfo

	return true, hlrec
}

func (hl *Hardlinks) SetHardLink(finfo os.FileInfo,
	record *quantumfs.DirectRecord) quantumfs.DirectoryRecord {

	// need locks to protect the map
	defer hl.hardLinkInfoMutex.Lock().Unlock()

	stat := finfo.Sys().(*syscall.Stat_t)

	// now that we have the lock, check to ensure nobody has created it already
	existingHl := hl.getHardlink_(finfo)
	if existingHl != nil {
		// ensure we increment the hardlink count
		hlinfo := hl.hardLinkInfoMap[stat.Ino]
		hlinfo.nlinks++
		hl.hardLinkInfoMap[stat.Ino] = hlinfo

		return existingHl
	}

	hl.hardLinkInfoMap[stat.Ino] = &HardLinkInfo{
		record: record,
		nlinks: 1,
	}

	// construct a thin directory record to represent
	// source of the hard link
	newDirRecord := quantumfs.NewDirectoryRecord()
	newDirRecord.SetType(quantumfs.ObjectTypeHardlink)
	newDirRecord.SetFilename(record.Filename())
	newDirRecord.SetFileId(record.FileId())

	return newDirRecord
}

func (hl *Hardlinks) writeHardLinkInfo(qctx *quantumfs.Ctx,
	ds quantumfs.DataStore) (*quantumfs.HardlinkEntry, error) {

	// entryIdx indexes into the metadata block
	entryNum := len(hl.hardLinkInfoMap)
	entryNum, hle := quantumfs.NewHardlinkEntry(entryNum)
	hleKey := quantumfs.EmptyDirKey
	entryIdx := 0
	var err error
	for _, hlinfo := range hl.hardLinkInfoMap {
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
