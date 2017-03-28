// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bufio"
import "os"
import "strconv"
import "strings"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

func modifyEntryWithAttr(c *ctx, newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	entry quantumfs.DirectoryRecord, updateMtime bool) {

	// Update the type if needed
	if newType != nil {
		entry.SetType(*newType)
		c.vlog("Type now %d", *newType)
	}

	valid := uint(attr.SetAttrInCommon.Valid)
	// We don't support file locks yet, but when we do we need
	// FATTR_LOCKOWNER

	var now quantumfs.Time
	if utils.BitAnyFlagSet(valid, fuse.FATTR_MTIME_NOW) ||
		!utils.BitFlagsSet(valid, fuse.FATTR_CTIME) || updateMtime {

		now = quantumfs.NewTime(time.Now())
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_MODE) {
		entry.SetPermissions(modeToPermissions(attr.Mode, 0))
		c.vlog("Permissions now %d Mode %d", entry.Permissions(), attr.Mode)
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_UID) {
		entry.SetOwner(quantumfs.ObjectUid(attr.Owner.Uid,
			c.fuseCtx.Owner.Uid))
		c.vlog("Owner now %d UID %d context %d", entry.Owner(),
			attr.Owner.Uid, c.fuseCtx.Owner.Uid)
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_GID) {
		entry.SetGroup(quantumfs.ObjectGid(attr.Owner.Gid,
			c.fuseCtx.Owner.Gid))
		c.vlog("Group now %d GID %d context %d", entry.Group(),
			attr.Owner.Gid, c.fuseCtx.Owner.Gid)
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_SIZE) {
		entry.SetSize(attr.Size)
		c.vlog("Size now %d", entry.Size())
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) {
		// atime is ignored and not stored
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_MTIME_NOW) {
		entry.SetModificationTime(now)
		c.vlog("ModificationTime now %d", entry.ModificationTime())
	} else if utils.BitFlagsSet(valid, fuse.FATTR_MTIME) {
		entry.SetModificationTime(
			quantumfs.NewTimeSeconds(attr.Mtime, attr.Mtimensec))
		c.vlog("ModificationTime now %d", entry.ModificationTime())
	} else if updateMtime {
		c.vlog("Updated mtime")
		entry.SetModificationTime(now)
	}

	if utils.BitFlagsSet(valid, fuse.FATTR_CTIME) {
		entry.SetContentTime(quantumfs.NewTimeSeconds(attr.Ctime,
			attr.Ctimensec))
		c.vlog("ContentTime now %d", entry.ContentTime())
	} else {
		// Since we've updated the file attributes we need to update at least
		// its ctime (unless we've explicitly set its ctime).
		c.vlog("Updated ctime")
		entry.SetContentTime(now)
	}
}

type DeferableMutex struct {
	lock sync.Mutex
}

func (df *DeferableMutex) Lock() *sync.Mutex {
	df.lock.Lock()
	return &df.lock
}

func (df *DeferableMutex) Unlock() {
	df.lock.Unlock()
}

// Return the lock via a tiny interface to prevent read/write lock/unlock mismatch
type NeedReadUnlock interface {
	RUnlock()
}

type NeedWriteUnlock interface {
	Unlock()
}

type DeferableRwMutex struct {
	lock sync.RWMutex
}

func (df *DeferableRwMutex) RLock() NeedReadUnlock {
	df.lock.RLock()
	return &df.lock
}

func (df *DeferableRwMutex) Lock() NeedWriteUnlock {
	df.lock.Lock()
	return &df.lock
}

func (df *DeferableRwMutex) RUnlock() {
	df.lock.RUnlock()
}

func (df *DeferableRwMutex) Unlock() {
	df.lock.Unlock()
}

// Return the fuse connection id for the filesystem mounted at the given path
func findFuseConnection(c *ctx, mountPath string) int {
	c.dlog("Finding FUSE Connection ID...")
	for i := 0; i < 100; i++ {
		c.dlog("Waiting for mount try %d...", i)
		file, err := os.Open("/proc/self/mountinfo")
		if err != nil {
			c.dlog("Failed opening mountinfo: %s", err.Error())
			return -1
		}
		defer file.Close()

		mountinfo := bufio.NewReader(file)

		for {
			bline, _, err := mountinfo.ReadLine()
			if err != nil {
				break
			}

			line := string(bline)

			if strings.Contains(line, mountPath) {
				fields := strings.SplitN(line, " ", 5)
				dev := strings.Split(fields[2], ":")[1]
				devInt, err := strconv.Atoi(dev)
				if err != nil {
					c.elog("Failed to convert dev to integer")
					return -1
				}
				c.vlog("Found mountId %d", devInt)
				return devInt
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
	c.elog("FUSE mount not found in time")
	return -1
}

func openPermission(c *ctx, inode Inode, flags_ uint32) bool {
	defer c.FuncIn("openPermission", "%d", inode.inodeNum()).out()

	record, error := inode.parentGetChildRecordCopy(c, inode.inodeNum())
	if error != nil {
		c.elog("%s", error.Error())
		return false
	}

	if c.fuseCtx.Owner.Uid == 0 {
		c.vlog("Root permission check, allowing")
		return true
	}

	flags := uint(flags_)

	c.vlog("Open permission check. Have %x, flags %x", record.Permissions(),
		flags)

	var userAccess bool
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		userAccess = utils.BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermReadOther|quantumfs.PermReadGroup|
				quantumfs.PermReadOwner)
	case syscall.O_WRONLY:
		userAccess = utils.BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermWriteOwner|quantumfs.PermWriteGroup|
				quantumfs.PermWriteOwner)
	case syscall.O_RDWR:
		userAccess = utils.BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermWriteOther|quantumfs.PermWriteGroup|
				quantumfs.PermWriteOwner|quantumfs.PermReadOther|
				quantumfs.PermReadGroup|quantumfs.PermReadOwner)
	}

	var execAccess bool
	if utils.BitFlagsSet(flags, FMODE_EXEC) {
		execAccess = utils.BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermExecOther|quantumfs.PermExecGroup|
				quantumfs.PermExecOwner|quantumfs.PermSUID|
				quantumfs.PermSGID)
	}

	success := userAccess || execAccess
	c.vlog("Permission check result %d", success)
	return success
}

func hasPermission(c *ctx, inode Inode, fileOwner uint32, checkUid uint32,
	checkFlags uint32, checkStickyBit bool) fuse.Status {

	var arg string
	if checkStickyBit {
		arg = "checkStickyBit, %x"
	} else {
		arg = "no checkStickyBit, %x"
	}
	defer c.FuncIn("hasPermission", arg, checkFlags).out()

	// If the directory is a workspace root, it is always permitted to modify the
	// children inodes because its permission is 777 (Hardcoded in
	// daemon/workspaceroot.go).
	if dir.self.isWorkspaceRoot() {
		c.vlog("Is WorkspaceRoot: OK")
		return fuse.OK
	}

	owner := c.fuseCtx.Owner
	dirRecord, err := dir.parentGetChildRecordCopy(c, dir.InodeCommon.id)
	if err != nil {
		c.wlog("Failed to find directory record in parent")
		return fuse.ENOENT
	}
	dirOwner := quantumfs.SystemUid(dirRecord.Owner(), owner.Uid)
	dirGroup := quantumfs.SystemGid(dirRecord.Group(), owner.Gid)
	permission := dirRecord.Permissions()

	// Root permission can bypass the permission, and the root is only verified
	// by uid
	if owner.Uid == 0 {
		c.vlog("User is root: OK")
		return fuse.OK
	}

	// Verify the permission of the directory in order to delete a child
	// If the sticky bit of the directory is set, the action can only be
	// performed by file's owner, directory's owner, or root user
	if checkStickyBit &&
		utils.BitFlagsSet(uint(permission), uint(syscall.S_ISVTX)) &&
		owner.Uid != fileOwner && owner.Uid != dirOwner {

		c.vlog("Sticky owners don't match: FAIL")
		return fuse.EACCES
	}

	// Get whether current user is OWNER/GRP/OTHER
	var permWX uint32
	if owner.Uid == dirOwner {
		permWX = syscall.S_IWUSR | syscall.S_IXUSR
		// Check the current directory having x and w permissions
		if utils.BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has owner write: OK")
			return fuse.OK
		}
	} else if owner.Gid == dirGroup {
		permWX = syscall.S_IWGRP | syscall.S_IXGRP
		if utils.BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has group write: OK")
			return fuse.OK
		}
	} else { // all the other
		permWX = syscall.S_IWOTH | syscall.S_IXOTH
		if utils.BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has other write: OK")
			return fuse.OK
		}
	}

	c.vlog("Directory::hasWritePermission %o vs %o", permWX, permission)
	return fuse.EACCES
}

