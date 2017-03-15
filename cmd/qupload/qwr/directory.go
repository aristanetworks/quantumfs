// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "path/filepath"
import "syscall"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

func WriteDirectory(base string, name string,
	childRecords []*quantumfs.DirectoryRecord,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, error) {

	rootDirEntry := quantumfs.NewDirectoryEntry()
	rootDirEntry.SetNext(quantumfs.EmptyDirKey)
	entryIdx := 0
	for _, child := range childRecords {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			rootDirEntry.SetNumEntries(entryIdx)
			key, err := writeBlob(rootDirEntry.Bytes(), quantumfs.KeyTypeMetadata, ds)
			if err != nil {
				return nil, err
			}
			rootDirEntry = quantumfs.NewDirectoryEntry()
			rootDirEntry.SetNext(key)
			entryIdx = 0
		}

		rootDirEntry.SetEntry(entryIdx, child)
		entryIdx++
	}

	rootDirEntry.SetNumEntries(entryIdx)
	key, err := writeBlob(rootDirEntry.Bytes(), quantumfs.KeyTypeMetadata, ds)
	if err != nil {
		return nil, err
	}

	//TODO(krishna): handle the xattrs setup on current directory
	var dirRecord *quantumfs.DirectoryRecord
	if name == "" {
		//root directory - mode 0755, size = 0 since dir size
		// is approximated by QFS based on dir entries
		dirRecord = createNewDirRecord("", 0755,
			0, 0, 0, 0,
			quantumfs.ObjectTypeDirectoryEntry,
			key)
		// xattrs cannot be saved in workspace root dir
	} else {
		finfo, serr := os.Lstat(filepath.Join(base, name))
		if serr != nil {
			return nil, serr
		}
		stat := finfo.Sys().(*syscall.Stat_t)
		dirRecord = createNewDirRecord(name, stat.Mode,
			uint32(stat.Rdev), 0,
			quantumfs.ObjectUid(stat.Uid, stat.Uid),
			quantumfs.ObjectGid(stat.Gid, stat.Gid),
			quantumfs.ObjectTypeDirectoryEntry,
			key)

		xattrsKey, xerr := WriteXAttrs(filepath.Join(base, name), ds)
		if xerr != nil {
			return nil, xerr
		}
		if !xattrsKey.IsEqualTo(quantumfs.EmptyBlockKey) {
			dirRecord.SetExtendedAttributes(xattrsKey)
		}

	}

	return dirRecord, nil
}

func createNewDirRecord(name string, mode uint32,
	rdev uint32, size uint64, uid quantumfs.UID,
	gid quantumfs.GID, objType quantumfs.ObjectType,
	key quantumfs.ObjectKey) *quantumfs.DirectoryRecord {

	now := time.Now()
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
	// QFS doesn't store Atime currently
	entry.SetContentTime(quantumfs.NewTime(now))
	entry.SetModificationTime(quantumfs.NewTime(now))

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
