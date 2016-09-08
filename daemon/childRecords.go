// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// childRecords is a type which encapsulates the mechanism by which child
// inodes are loaded only as needed, so that users don't need to deal with it.
// Directory uses this class.

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
	dir     *Directory
}

func newChildRecords(dirParent *Directory) childRecords {
	return childRecords{
		data:    nil,
		entries: make(map[string]*quantumfs.DirectoryRecord, 0),
		dir:     dirParent,
	}
}

func newChildRecordsData(lenHint int) *childRecordsData {
	var rtn childRecordsData
	rtn.fileToInode = make(map[string]InodeId, lenHint)
	rtn.records = make(map[InodeId]*quantumfs.DirectoryRecord, lenHint)
	rtn.dirtyInodes = make(map[InodeId]InodeId, lenHint)

	return &rtn
}

func (cr *childRecords) loadData_(c *ctx) {
	// Data already loaded, nothing to do
	if cr.data != nil {
		return
	}

	cr.data = newChildRecordsData(len(cr.entries))
	// go through the entries and load the children
	for _, v := range cr.entries {
		cr.instantiateChild_(c, v)
	}
}

// The directory must be exclusively locked (or unlisted)
func (cr *childRecords) instantiateChild_(c *ctx, entry *quantumfs.DirectoryRecord) {
	inodeId := c.qfs.newInodeId()
	cr.data.fileToInode[entry.Filename()] = inodeId
	cr.data.records[inodeId] = entry
	var constructor InodeConstructor
	switch entry.Type() {
	default:
		c.elog("Unknown InodeConstructor type: %d", entry.Type())
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

	c.qfs.setInode(c, inodeId, constructor(c, entry.ID(), entry.Size(),
		inodeId, cr.dir, 0, 0, nil))
}

func (cr *childRecords) insertRecord(c *ctx, inode InodeId,
	entry *quantumfs.DirectoryRecord) {

	cr.loadData_(c)

	cr.entries[entry.Filename()] = entry
	cr.data.fileToInode[entry.Filename()] = inode
	cr.data.records[inode] = entry
	// being inserted means you're dirty and need to be synced
	cr.data.dirtyInodes[inode] = inode
}

func (cr *childRecords) setRecord(c *ctx, entry *quantumfs.DirectoryRecord) {
	cr.entries[entry.Filename()] = entry

	// If we've loaded children, then we need to load new ones also
	if cr.data != nil {
		inode, childLoaded := cr.data.fileToInode[entry.Filename()]

		if !childLoaded {
			cr.instantiateChild_(c, entry)
		} else {
			cr.data.records[inode] = entry
		}
	}
}

func (cr *childRecords) setLoadedRecord(c *ctx, inode InodeId,
	entry *quantumfs.DirectoryRecord) {

	// If a child record has been loaded for us, we need to load the rest
	cr.loadData_(c)

	cr.entries[entry.Filename()] = entry
	cr.data.fileToInode[entry.Filename()] = inode
	cr.data.records[inode] = entry
}

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

func (cr *childRecords) getInode(c *ctx, filename string) (InodeId, bool) {
	cr.loadData_(c)

	inode, ok := cr.data.fileToInode[filename]
	return inode, ok
}

func (cr *childRecords) getNameRecord(c *ctx,
	filename string) (dr *quantumfs.DirectoryRecord, exists bool) {
	
	inode, ok := cr.data.fileToInode[filename]
	if !ok {
		return nil, false
	}

	return cr.getRecord(c, inode)
}

func (cr *childRecords) getRecord(c *ctx,
	inodeNum InodeId) (dr *quantumfs.DirectoryRecord, exists bool) {

	cr.loadData_(c)

	if value, ok := cr.data.records[inodeNum]; ok {
		return value, true
	}

	return nil, false
}

func (cr *childRecords) getRecords() map[string]*quantumfs.DirectoryRecord {
	return cr.entries
}

func (cr *childRecords) dirty(c *ctx, inodeNum InodeId) {
	cr.loadData_(c)

	cr.data.dirtyInodes[inodeNum] = inodeNum
}

func (cr *childRecords) rename(c *ctx, oldName string, newName string) {
	if oldName == newName {
		return
	}

	cr.loadData_(c)

	oldInodeId := cr.data.fileToInode[oldName]
	// If a file already exists with newName, we need to clean it up
	cleanupInodeId := cr.data.fileToInode[newName]

	cr.entries[newName] = cr.data.records[oldInodeId]
	cr.data.records[oldInodeId].SetFilename(newName)

	cr.data.fileToInode[newName] = oldInodeId
	delete(cr.data.fileToInode, oldName)
	delete(cr.entries, oldName)

	// cleanup / remove any existing inode with that name
	delete(cr.data.records, cleanupInodeId)
	delete(cr.data.dirtyInodes, cleanupInodeId)
}

func (cr *childRecords) popDirtyInodes() map[InodeId]InodeId {
	rtn := cr.data.dirtyInodes
	cr.data.dirtyInodes = make(map[InodeId]InodeId, 0)

	return rtn
}
