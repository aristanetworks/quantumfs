// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

func WriteDirectory(qctx *quantumfs.Ctx, path string, info os.FileInfo,
	childRecords []quantumfs.DirectoryRecord,
	ds quantumfs.DataStore) (rtn quantumfs.DirectoryRecord, bytesWritten uint64,
	err error) {

	// To promote dedupe, sort the directory
	// records by Filename before writing DirectoryEntry.
	// Sorting ensures that, for a given set of records in
	// childRecords, even in the presence of parallelism,
	// the DirectoryEntry object will be same.
	// Note: This is not same as source directory but thats ok.
	quantumfs.SortDirectoryRecords(childRecords)

	entryNum := len(childRecords)
	entryNum, dirEntry := quantumfs.NewDirectoryEntry(entryNum)
	dirEntry.SetNext(quantumfs.EmptyDirKey)
	entryIdx := 0
	totalWritten := uint64(0)
	for _, child := range childRecords {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			dirEntry.SetNumEntries(entryIdx)
			key, err := writeBlock(qctx, dirEntry.Bytes(),
				quantumfs.KeyTypeMetadata, ds)
			if err != nil {
				return nil, 0,
					fmt.Errorf("WriteDirectory %q "+
						"failed: %v", path, err)
			}
			totalWritten += uint64(len(dirEntry.Bytes()))
			entryNum, dirEntry = quantumfs.NewDirectoryEntry(entryNum)
			dirEntry.SetNext(key)
			entryIdx = 0
		}
		dirEntry.SetEntry(entryIdx, child.Publishable())
		entryIdx++
	}

	dirEntry.SetNumEntries(entryIdx)
	key, err := writeBlock(qctx, dirEntry.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if err != nil {
		return nil, 0, fmt.Errorf("WriteDirectory %q failed: %v",
			path, err)
	}
	totalWritten += uint64(len(dirEntry.Bytes()))

	stat := info.Sys().(*syscall.Stat_t)
	dirRecord := CreateNewDirRecord(info.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(cap(childRecords)),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		quantumfs.ObjectTypeDirectory,
		// retain time of the input directory
		quantumfs.NewTime(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)),
		quantumfs.NewTime(time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)),
		key)

	xattrsKey, attrsWritten, xerr := WriteXAttrs(qctx, path, ds)
	if xerr != nil {
		return nil, 0, xerr
	}
	if !xattrsKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		dirRecord.SetExtendedAttributes(xattrsKey)
	}
	totalWritten += attrsWritten

	return dirRecord, totalWritten, nil
}

func CreateNewDirRecord(name string, mode uint32,
	rdev uint32, size uint64, uid quantumfs.UID,
	gid quantumfs.GID, objType quantumfs.ObjectType,
	mtime quantumfs.Time, ctime quantumfs.Time,
	key quantumfs.ObjectKey) quantumfs.DirectoryRecord {

	entry := quantumfs.NewDirectoryRecord()
	entry.SetFilename(name)
	entry.SetID(key)
	entry.SetType(objType)
	// we retain the permissions for the source dir/file
	// and ignore umask of writer
	entry.SetPermissions(modeToPermissions(mode, 0))
	entry.SetOwner(uid)
	entry.SetGroup(gid)
	entry.SetSize(size)
	entry.SetExtendedAttributes(quantumfs.EmptyBlockKey)
	// QFS doesn't store Atime since it's too expensive
	entry.SetContentTime(ctime)
	entry.SetModificationTime(mtime)
	entry.SetFileId(quantumfs.GenerateUniqueFileId())

	return entry
}

func modeToPermissions(mode uint32, umask uint32) uint32 {
	var permissions uint32
	mode = mode & ^umask

	if utils.BitFlagsSet(uint(mode), syscall.S_IXOTH) {
		permissions |= quantumfs.PermExecOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWOTH) {
		permissions |= quantumfs.PermWriteOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IROTH) {
		permissions |= quantumfs.PermReadOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IXGRP) {
		permissions |= quantumfs.PermExecGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWGRP) {
		permissions |= quantumfs.PermWriteGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IRGRP) {
		permissions |= quantumfs.PermReadGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IXUSR) {
		permissions |= quantumfs.PermExecOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWUSR) {
		permissions |= quantumfs.PermWriteOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IRUSR) {
		permissions |= quantumfs.PermReadOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISVTX) {
		permissions |= quantumfs.PermSticky
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISGID) {
		permissions |= quantumfs.PermSGID
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISUID) {
		permissions |= quantumfs.PermSUID
	}

	return permissions
}
