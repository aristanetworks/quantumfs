// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This class is to provide inodes with a concurrency safe way to access their parent
// Note: No inode should have direct access to their parent due to races with mv
package daemon

import "fmt"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"


type lockedParent struct {
	lock	 	DeferableRwMutex // Protects parent_
	parentId	InodeId
}

// Must have the lockedParent.lock Lock()-ed.
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
	defer lp.lock.Lock().Unlock()
	
	lp.parent_(c).markAccessed(c, path, created)
}

// For use only with uninstantiateChain in mux. In almost all cases we should not
// allow direct access to the parent inode because it could change outside this lock.
// Uninstantiate chain is only okay because of the treeLock
func (lp *lockedParent) uninstantiateChild(c *ctx, inode Inode,
	qfs *QuantumFs) (parent Inode) {

	defer lp.lock.Lock().Unlock()

	inodeNum := inode.inodeNum()

	// Do nothing if we're orphaned
	if inodeNum == lp.parentId {
		return nil
	}

	key := inode.flush(c)

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

	defer lp.lock.Lock().Unlock()

	// We want to ensure that the orphan check and the parent sync are done
	// under the same lock
	if childId == lp.parentId {
		c.vlog("Not flushing orphaned inode")
		return
	}

	// publish before we sync, once we know it's safe
	baseLayerId := publishFn()

	lp.parent_(c).syncChild(c, childId, baseLayerId)
}

func (lp *lockedParent) childIsOrphaned(childId InodeId) bool {
	defer lp.lock.RLock().RUnlock()

	return childId == lp.parentId
}

func (lp *lockedParent) setParent(newParent InodeId) {
	defer lp.lock.Lock().Unlock()

	lp.parentId = newParent
}

func (lp *lockedParent) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).setChildAttr(c, inodeNum, newType, attr, out,
		updateMtime)
}

func (lp *lockedParent) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).getChildXAttrSize(c, inodeNum, attr)
}

func (lp *lockedParent) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).getChildXAttrData(c, inodeNum, attr)
}

func (lp *lockedParent) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).listChildXAttr(c, inodeNum)
}

func (lp *lockedParent) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).setChildXAttr(c, inodeNum, attr, data)
}

func (lp *lockedParent) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).removeChildXAttr(c, inodeNum, attr)
}

func (lp *lockedParent) getChildRecord(c *ctx,
	inodeNum InodeId) (DirectoryRecordIf, error) {

	defer lp.lock.Lock().Unlock()
	return lp.parent_(c).getChildRecord(c, inodeNum)
}

// When iterating up the directory tree, we need to lock parents as we go,
// otherwise part of the chain we've iterated past could be moved and change
// what we should have returned here
func (lp *lockedParent) hasAncestor(c *ctx, ancestor Inode) bool {
	defer lp.lock.Lock().Unlock()

	if ancestor.inodeNum() == lp.parentId {
		return true
	}

	if lp.parentId == quantumfs.InodeIdInvalid {
		return false
	}

	return lp.parent_(c).lockedParent().hasAncestor(c, ancestor)
}
