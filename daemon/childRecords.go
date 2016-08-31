// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// childRecords is a type which encapsulates the mechanism by which child
// inodes are loaded only as needed, so that users don't need to deal with it

import "github.com/aristanetworks/quantumfs"

type childRecordsData struct {
	fileToInode map[string]InodeId
	records     map[InodeId]*quantumfs.DirectoryRecord
	dirtyInodes map[InodeId]InodeId // set of children which need sync
}

type childRecords struct {
	// Only load record data when we absolutely have to
	data    *childRecordsData
	entries map[string]*quantumfs.DirectoryRecord
	c       *ctx
	dir     *Directory
}

func newChildRecords(c *ctx, dirParent *Directory) childRecords {
	return childRecords{
		data:    nil,
		entries: make(map[string]*quantumfs.DirectoryRecord, 0),
		c:       c,
		dir:     dirParent,
	}
}

func newChildRecordsData() *childRecordsData {
	var rtn childRecordsData
	rtn.fileToInode = make(map[string]InodeId, 0)
	rtn.records = make(map[InodeId]*quantumfs.DirectoryRecord, 0)
	rtn.dirtyInodes = make(map[InodeId]InodeId, 0)

	return &rtn
}

func (cr *childRecords) loadData_() {
	// Data already loaded, nothing to do
	if cr.data != nil {
		return
	}

	cr.data = newChildRecordsData()
	// go through the entries and load the children
	for _, v := range cr.entries {
		cr.loadChild_(v)
	}
}

// The directory must be exclusively locked (or unlisted)
func (cr *childRecords) loadChild_(entry *quantumfs.DirectoryRecord) {
	inodeId := cr.c.qfs.newInodeId()
	cr.data.fileToInode[entry.Filename()] = inodeId
	cr.data.records[inodeId] = entry
	var constructor InodeConstructor
	switch entry.Type() {
	default:
		cr.c.elog("Unknown InodeConstructor type: %d", entry.Type())
		panic("Unknown InodeConstructor type")
	case quantumfs.ObjectTypeDirectoryEntry:
		constructor = newDirectory
	case quantumfs.ObjectTypeSmallFile:
		constructor = newSmallFile
	case quantumfs.ObjectTypeMediumFile:
		constructor = newMediumFile
	case quantumfs.ObjectTypeLargeFile:
		constructor = newLargeFile
	case quantumfs.ObjectTypeVeryLargeFile:
		constructor = newVeryLargeFile
	case quantumfs.ObjectTypeSymlink:
		constructor = newSymlink
	case quantumfs.ObjectTypeSpecial:
		constructor = newSpecial
	}

	cr.c.qfs.setInode(cr.c, inodeId, constructor(cr.c, entry.ID(), entry.Size(),
		inodeId, cr.dir, 0, 0, nil))
}

func (cr *childRecords) insertRecord(inode InodeId,
	entry *quantumfs.DirectoryRecord) {

	cr.loadData_()

	cr.entries[entry.Filename()] = entry
	cr.data.fileToInode[entry.Filename()] = inode
	cr.data.records[inode] = entry
	// being inserted means you're dirty and need to be synced
	cr.data.dirtyInodes[inode] = inode
}

// childRecords functions which do *not* require loaded data
func (cr *childRecords) setRecord(entry *quantumfs.DirectoryRecord) {
	cr.entries[entry.Filename()] = entry

	// If we've loaded children, then we need to load new ones also
	if cr.data != nil {
		inode, childLoaded := cr.data.fileToInode[entry.Filename()]

		if !childLoaded {
			cr.loadChild_(entry)
		} else {
			cr.data.records[inode] = entry
		}
	}
}

func (cr *childRecords) setLoadedRecord(inode InodeId,
	entry *quantumfs.DirectoryRecord) {

	// If a child record has been loaded for us, we need to load the rest
	cr.loadData_()

	cr.entries[entry.Filename()] = entry
	cr.data.fileToInode[entry.Filename()] = inode
	cr.data.records[inode] = entry
}

// childRecords functions which require loaded data
func (cr *childRecords) delete(filename string) {
	if cr.data != nil {
		inodeNum, exists := cr.data.fileToInode[filename]
		if exists {
			delete(cr.data.records, inodeNum)
			delete(cr.data.dirtyInodes, inodeNum)
			delete(cr.data.fileToInode, filename)
		}
	}

	delete(cr.entries, filename)
}

func (cr *childRecords) count() int {
	return len(cr.entries)
}

func (cr *childRecords) countChildDirs() int {
	var childDirectories int
	for _, entry := range cr.entries {
		if entry.Type() == quantumfs.ObjectTypeDirectoryEntry {
			childDirectories++
		}
	}

	return childDirectories
}

func (cr *childRecords) getInode(filename string) (InodeId, bool) {
	cr.loadData_()

	inode, ok := cr.data.fileToInode[filename]
	return inode, ok
}

func (cr *childRecords) getRecord(inodeNum InodeId) (dr *quantumfs.DirectoryRecord,
	exists bool) {

	cr.loadData_()

	if value, ok := cr.data.records[inodeNum]; ok {
		return value, true
	}

	return nil, false
}

func (cr *childRecords) getRecords() map[string]*quantumfs.DirectoryRecord {
	return cr.entries
}

func (cr *childRecords) dirty(inodeNum InodeId) {
	cr.loadData_()

	cr.data.dirtyInodes[inodeNum] = inodeNum
}

func (cr *childRecords) rename(oldName string, newName string) {
	if oldName == newName {
		return
	}

	cr.loadData_()

	oldInodeId := cr.data.fileToInode[oldName]
	newInodeId := cr.data.fileToInode[newName]

	cr.entries[newName] = cr.data.records[oldInodeId]
	cr.data.records[oldInodeId].SetFilename(newName)

	cr.data.fileToInode[newName] = oldInodeId
	delete(cr.data.fileToInode, oldName)
	delete(cr.entries, oldName)

	// cleanup / remove any existing inode with that name
	delete(cr.data.records, newInodeId)
	delete(cr.data.dirtyInodes, newInodeId)
}

func (cr *childRecords) popDirtyInodes() map[InodeId]InodeId {
	rtn := cr.data.dirtyInodes
	cr.data.dirtyInodes = make(map[InodeId]InodeId, 0)

	return rtn
}
