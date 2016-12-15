// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon


// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
// Note: NOT concurrency safe - requires an external mutex to protect it
type ChildMap struct {

	children	map[string]InodeId
	dirtyChildren	map[InodeId]InodeId // a set

	childrenRecords map[InodeId]DirectoryRecordIf
}

func newChildMap(numEntries int) *ChildMap {
	return &ChildMap {
		children:	make(map[string]InodeId, numEntries),
		dirtyChildren:	make(map[InodeId]InodeId, 0),
		childrenRecords: make(map[InodeId]DirectoryRecordIf, numEntries),
	}
}

// The directory parent must be exclusively locked
func (cmap *ChildMap) newChild(c *ctx, entry DirectoryRecordIf) InodeId {
	inodeId := c.qfs.newInodeId()
	setChild(c, entry, inodeid)

	return inodeId
}

func (cmap *ChildMap) setChild(c *ctx, entry DirectoryRecordIf, inodeId InodeId) {
	cmap.children[entry.Filename()] = inodeId
	// child is not dirty by default

	cmap.childrenRecords[inodeId] = entry
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.childrenRecords))
}

func (cmap *ChildMap) deleteChild(inodeNum InodeId) DirectoryRecordIf {
	record := cmap.childrenRecords[inodeNum]
	delete(cmap.childrenRecords, inodeNum)
	delete(cmap.dirtyChildren_, inodeNum)
	delete(cmap.children, record.Filename())

	return record
}

func (cmap *ChildMap) setDirty(c *ctx, inodeNum InodeId) {
	if _, exists := cmap.childrenRecords[inodeNum]; !exists {
		c.elog("Attempt to dirty child that doesn't exist: %d", inodeNum)
		return
	}

	cmap.dirtyChildren_[inodeNum] = inodeNum
}

func (cmap *ChildMap) inodeNum(name string) InodeId {
	if inodeId, exists := cmap.children[name]; exists {
		return inodeId
	}

	panic("No valid child: %s", name)
}

func (cmap *ChildMap) inodes() []InodeId {
	rtn := make([]InodeId, len(cmap.children))
	for _, v := range cmap.children {
		rtn = append(rtn, v)
	}

	return rtn
}

func (cmap *ChildMap) records() []DirectoryRecordIf {
	rtn := make([]DirectoryRecordIf, len(cmap.childrenRecords))
	for _, i := range cmap.childrenRecords {
		rtn = append(rtn, i)
	}

	return rtn
}

func (cmap *ChildMap) record(inodeNum InodeId) DirectoryRecordIf {
	entry, exists := cmap.childrenRecords[inodeNum]
	if !exists {
		return nil
	}

	return entry
}

func (cmap *ChildMap) recordByName(c *ctx, name string) DirectoryRecordIf {
	inodeNum, exists := cmap.children[name]
	if !exists {
		return nil
	}

	entry, exists := cmap.childrenRecords[inodeNum]
	if !exists {
		c.elog("child record map mismatch %d %s", inodeNum, name)
		return nil
	}

	return entry
}
