// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This class is to provide inodes with a concurrency safe way to access their parent
// Note: No inode should have direct access to their parent due to races with mv
package daemon

import "fmt"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type lockedParent struct {
	lock     DeferableRwMutex // Protects parent_
	parentId InodeId
}

// Must have the lockedParent.lock RLock()-ed.
func (lp *lockedParent) parent_(c *ctx) Inode {
	parent := c.qfs.inodeNoInstantiate(c, lp.parentId)
	if parent == nil {
		c.elog("Parent was unloaded before child"+
			"! %d", lp.parentId)
		parent = c.qfs.inode(c, lp.parentId)
	}

	return parent
}

func (lp *lockedParent) markAccessed(c *ctx, path string, created bool) {
	defer lp.lock.RLock().RUnlock()

	lp.parent_(c).markAccessed(c, path, created)
}

// For use only with uninstantiateChain in mux. In almost all cases we should not
// allow direct access to the parent inode because it could change outside this lock.
// Uninstantiate chain is only okay because of the treeLock
func (lp *lockedParent) uninstantiateChild(c *ctx, inodeNum InodeId,
	key quantumfs.ObjectKey, qfs *QuantumFs) (parent Inode) {

	defer lp.lock.RLock().RUnlock()

	// Do nothing if we're orphaned
	if lp.childIsOrphaned_(inodeNum) {
		return nil
	}

	parent = qfs.inodeNoInstantiate(c, lp.parentId)
	if parent == nil {
		panic(fmt.Sprintf("Parent was unloaded before child"+
			"! %d %d", lp.parentId, inodeNum))
	}

	parent.syncChild(c, inodeNum, key)

	qfs.addUninstantiated(c, []InodeId{inodeNum}, lp.parentId)

	return parent
}

func (lp *lockedParent) syncChild(c *ctx, childId InodeId,
	publishFn func() quantumfs.ObjectKey) {

	defer lp.lock.RLock().RUnlock()

	// We want to ensure that the orphan check and the parent sync are done
	// under the same lock
	if lp.childIsOrphaned_(childId) {
		c.vlog("Not flushing orphaned inode")
		return
	}

	// publish before we sync, once we know it's safe
	baseLayerId := publishFn()

	lp.parent_(c).syncChild(c, childId, baseLayerId)
}

func (lp *lockedParent) childIsOrphaned(childId InodeId) bool {
	defer lp.lock.RLock().RUnlock()

	return lp.childIsOrphaned_(childId)
}

// lp lock must be RLocked
func (lp *lockedParent) childIsOrphaned_(childId InodeId) bool {
	return childId == lp.parentId
}

func (lp *lockedParent) setParent(newParent InodeId) {
	defer lp.lock.Lock().Unlock()

	lp.parentId = newParent
}

func (lp *lockedParent) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).setChildAttr(c, inodeNum, newType, attr, out,
		updateMtime)
}

func (lp *lockedParent) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).getChildXAttrSize(c, inodeNum, attr)
}

func (lp *lockedParent) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).getChildXAttrData(c, inodeNum, attr)
}

func (lp *lockedParent) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).listChildXAttr(c, inodeNum)
}

func (lp *lockedParent) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).setChildXAttr(c, inodeNum, attr, data)
}

func (lp *lockedParent) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).removeChildXAttr(c, inodeNum, attr)
}

func (lp *lockedParent) getChildRecord(c *ctx,
	inodeNum InodeId) (DirectoryRecordIf, error) {

	defer lp.lock.RLock().RUnlock()
	return lp.parent_(c).getChildRecord(c, inodeNum)
}

// When iterating up the directory tree, we need to lock parents as we go,
// otherwise part of the chain we've iterated past could be moved and change
// what we should have returned here
func (lp *lockedParent) hasAncestor(c *ctx, ancestor Inode) bool {
	defer lp.lock.RLock().RUnlock()

	if ancestor.inodeNum() == lp.parentId {
		return true
	}

	if lp.parentId == quantumfs.InodeIdInvalid {
		return false
	}

	return lp.parent_(c).lockedParent().hasAncestor(c, ancestor)
}

func (lp *lockedParent) deleteChild(c *ctx, toDelete Inode,
	deleteFromParent func() (toOrphan DirectoryRecordIf,
		err fuse.Status)) fuse.Status {

	defer lp.lock.Lock().Unlock()

	// After we've locked the child, we can safely go UP and lock our parent
	toOrphan, err := deleteFromParent()
	if toOrphan == nil {
		// no orphan-ing desired here (hardlink or error)
		return err
	}

	if file, isFile := toDelete.(*File); isFile {
		file.setChildRecord(c, toOrphan)
	}
	// orphan ourselves
	lp.parentId = toDelete.inodeNum()
	c.vlog("Orphaned inode %d", toDelete.inodeNum())

	return fuse.OK
}

func (lp *lockedParent) checkLinkReparent(c *ctx, childId InodeId,
	parent *Directory) {

	defer lp.lock.Lock().Unlock()
	parent.lock.Lock()
	defer parent.lock.Unlock()
	defer parent.childRecordLock.Lock().Unlock()

	// Check again, now with the locks, if this is still a child
	record := parent.children.record(childId)
	if record == nil || record.Type() != quantumfs.ObjectTypeHardlink {
		// no hardlink record here, nothing to do
		return
	}

	link := record.(*Hardlink)

	// This may need to be turned back into a normal file
	newRecord, inodeId := parent.wsr.removeHardlink(c, link.linkId)

	if newRecord == nil && inodeId == quantumfs.InodeIdInvalid {
		// wsr says hardlink isn't ready for removal yet
		return
	}

	// reparent the child, the owner of lp, to the given parent
	lp.parentId = parent.inodeNum()

	// Ensure that we update this version of the record with this instance
	// of the hardlink's information
	newRecord.SetFilename(link.Filename())

	// Here we do the opposite of makeHardlink DOWN - we re-insert it
	parent.children.loadChild(c, newRecord, inodeId)
	parent.dirty(c)
}
