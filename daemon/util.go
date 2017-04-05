// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bufio"
import "os"
import "strconv"
import "strings"
import "syscall"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

const R_OK = 4
const W_OK = 2
const X_OK = 1
const F_OK = 0

func modifyEntryWithAttr(c *ctx, newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	entry quantumfs.DirectoryRecord, updateMtime bool) {

	defer c.funcIn("modifyEntryWithAttr").out()

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

// Return the fuse connection id for the filesystem mounted at the given path
func findFuseConnection(c *ctx, mountPath string) int {
	defer c.FuncIn("findFuseConnection", "mountPath %s", mountPath)
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

const permReadAll = quantumfs.PermReadOther | quantumfs.PermReadGroup |
	quantumfs.PermReadOwner
const permWriteAll = quantumfs.PermWriteOther | quantumfs.PermWriteGroup |
	quantumfs.PermWriteOwner
const permExecAll = quantumfs.PermExecOther | quantumfs.PermExecGroup |
	quantumfs.PermExecOwner

func hasAccessPermission(c *ctx, inode Inode, mode uint32, uid uint32,
	gid uint32) fuse.Status {

	// translate access flags into permission flags and return the result
	var checkFlags uint32
	if mode&R_OK != 0 {
		checkFlags |= permReadAll
	}

	if mode&W_OK != 0 {
		checkFlags |= permWriteAll
	}

	if mode&X_OK != 0 {
		checkFlags |= permExecAll
	}

	return hasPermissionIds(c, inode, uid, gid, checkFlags, -1)
}

func hasDirectoryWritePermSticky(c *ctx, inode Inode,
	childOwner quantumfs.UID) fuse.Status {

	checkFlags := uint32(permWriteAll | permExecAll)
	owner := c.fuseCtx.Owner
	return hasPermissionIds(c, inode, owner.Uid, owner.Gid, checkFlags,
		int32(childOwner))
}

func hasDirectoryWritePerm(c *ctx, inode Inode) fuse.Status {
	// Directories require execute permission in order to traverse them.
	// So, we must check both write and execute bits

	checkFlags := uint32(permWriteAll | permExecAll)
	owner := c.fuseCtx.Owner
	return hasPermissionIds(c, inode, owner.Uid, owner.Gid, checkFlags, -1)
}

func hasPermissionOpenFlags(c *ctx, inode Inode, openFlags uint32) fuse.Status {

	// convert open flags into permission ones
	checkFlags := uint32(0)
	switch openFlags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		checkFlags = permReadAll
	case syscall.O_WRONLY:
		checkFlags = permWriteAll
	case syscall.O_RDWR:
		checkFlags = permReadAll | permWriteAll
	}

	if utils.BitFlagsSet(uint(openFlags), FMODE_EXEC) {
		checkFlags |= permExecAll | quantumfs.PermSUID | quantumfs.PermSGID
	}

	owner := c.fuseCtx.Owner
	return hasPermissionIds(c, inode, owner.Uid, owner.Gid, checkFlags, -1)
}

func hasPermissionIds(c *ctx, inode Inode, checkUid uint32,
	checkGid uint32, checkFlags uint32, stickyAltOwner int32) fuse.Status {

	defer c.FuncIn("hasPermissionIds", "%d %d %d %o", checkUid, checkGid,
		stickyAltOwner, checkFlags).out()

	// Root permission can bypass the permission, and the root is only verified
	// by uid
	if checkUid == 0 {
		c.vlog("User is root: OK")
		return fuse.OK
	}

	// If the inode is a workspace root, it is always permitted to modify the
	// children inodes because its permission is 777 (Hardcoded in
	// daemon/workspaceroot.go).
	if inode.isWorkspaceRoot() {
		c.vlog("Is WorkspaceRoot: OK")
		return fuse.OK
	}

	record, err := inode.parentGetChildRecordCopy(c, inode.inodeNum())
	if err != nil {
		c.wlog("Failed to find record in parent")
		return fuse.ENOENT
	}
	inodeOwner := quantumfs.SystemUid(record.Owner(), checkUid)
	inodeGroup := quantumfs.SystemGid(record.Group(), checkGid)
	permission := record.Permissions()

	// Verify the permission of the inode in order to delete a child
	// If the sticky bit of a directory is set, the action can only be
	// performed by file's owner, directory's owner, or root user
	if stickyAltOwner >= 0 {
		stickyUid := quantumfs.SystemUid(quantumfs.UID(stickyAltOwner),
			checkUid)

		if record.Type() == quantumfs.ObjectTypeDirectoryEntry &&
			utils.BitFlagsSet(uint(permission), quantumfs.PermSticky) &&
			checkUid != inodeOwner && checkUid != stickyUid {

			c.vlog("Sticky owners don't match: FAIL")
			return fuse.EACCES
		}
	}

	// Get whether current user is OWNER/GRP/OTHER
	var permMask uint32
	if checkUid == inodeOwner {
		permMask = quantumfs.PermReadOwner | quantumfs.PermWriteOwner |
			quantumfs.PermExecOwner
	} else if checkGid == inodeGroup {
		permMask = quantumfs.PermReadGroup | quantumfs.PermWriteGroup |
			quantumfs.PermExecGroup
	} else { // all the other
		permMask = quantumfs.PermReadOther | quantumfs.PermWriteOther |
			quantumfs.PermExecOther
	}

	if utils.BitFlagsSet(uint(permission), uint(checkFlags&permMask)) {
		c.vlog("Has permission: OK. %o %o %o", checkFlags, permMask,
			permission)
		return fuse.OK
	}

	// If execute permissions are lacking, but the file has SUID/SGID, then we
	// allow it. This may not be correct behavior, but it's what we've been doing
	if utils.BitAnyFlagSet(uint(permission), uint(quantumfs.PermSUID|
		quantumfs.PermSGID&checkFlags)) {

		c.vlog("SUID/SGID set, Permission OK")
		return fuse.OK
	}

	c.vlog("hasPermissionIds (%o & %o) vs %o", checkFlags, permMask, permission)
	return fuse.EACCES
}
