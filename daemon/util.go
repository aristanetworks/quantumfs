// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bufio"
import "bytes"
import "os"
import "strconv"
import "strings"
import "syscall"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// A number of utility functions. It'd be nice to create packages for these
// elsewhere. Maybe a 'bit' package.

const R_OK = 4
const W_OK = 2
const X_OK = 1
const F_OK = 0

// Given a bitflag field and an integer of flags, return whether the flags are set or
// not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}

// Given a bitflag field and an integer of flags, return whether any flag is set or
// not as a boolean.
func BitAnyFlagSet(field uint, flags uint) bool {
	if field&flags != 0 {
		return true
	}
	return false
}

// Convert the given null terminated byte array into a string
func BytesToString(data []byte) string {
	length := bytes.IndexByte(data, 0)
	if length == -1 {
		length = len(data)
	}
	return string(data[:length])
}

// Convert the given null terminated string into a [256]byte array
func StringToBytes256(data string) [256]byte {
	var out [256]byte
	in := []byte(data)
	for i := range in {
		out[i] = in[i]
	}

	return out
}

// Given an integer, return the number of blocks of the given size necessary to
// contain it.
func BlocksRoundUp(len uint64, blockSize uint64) uint64 {
	blocks := len / blockSize
	if len%blockSize != 0 {
		blocks++
	}

	return blocks
}

// Panic with the given message if the condition isn't true.
func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}

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
	if BitAnyFlagSet(valid, fuse.FATTR_MTIME_NOW) ||
		!BitFlagsSet(valid, fuse.FATTR_CTIME) || updateMtime {

		now = quantumfs.NewTime(time.Now())
	}

	if BitFlagsSet(valid, fuse.FATTR_MODE) {
		entry.SetPermissions(modeToPermissions(attr.Mode, 0))
		c.vlog("Permissions now %d Mode %d", entry.Permissions(), attr.Mode)
	}

	if BitFlagsSet(valid, fuse.FATTR_UID) {
		entry.SetOwner(quantumfs.ObjectUid(attr.Owner.Uid,
			c.fuseCtx.Owner.Uid))
		c.vlog("Owner now %d UID %d context %d", entry.Owner(),
			attr.Owner.Uid, c.fuseCtx.Owner.Uid)
	}

	if BitFlagsSet(valid, fuse.FATTR_GID) {
		entry.SetGroup(quantumfs.ObjectGid(attr.Owner.Gid,
			c.fuseCtx.Owner.Gid))
		c.vlog("Group now %d GID %d context %d", entry.Group(),
			attr.Owner.Gid, c.fuseCtx.Owner.Gid)
	}

	if BitFlagsSet(valid, fuse.FATTR_SIZE) {
		entry.SetSize(attr.Size)
		c.vlog("Size now %d", entry.Size())
	}

	if BitFlagsSet(valid, fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) {
		// atime is ignored and not stored
	}

	if BitFlagsSet(valid, fuse.FATTR_MTIME_NOW) {
		entry.SetModificationTime(now)
		c.vlog("ModificationTime now %d", entry.ModificationTime())
	} else if BitFlagsSet(valid, fuse.FATTR_MTIME) {
		entry.SetModificationTime(
			quantumfs.NewTimeSeconds(attr.Mtime, attr.Mtimensec))
		c.vlog("ModificationTime now %d", entry.ModificationTime())
	} else if updateMtime {
		c.vlog("Updated mtime")
		entry.SetModificationTime(now)
	}

	if BitFlagsSet(valid, fuse.FATTR_CTIME) {
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
	defer c.FuncIn("File::openPermission", "%d", inode.inodeNum()).out()

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
		userAccess = BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermReadOther|quantumfs.PermReadGroup|
				quantumfs.PermReadOwner)
	case syscall.O_WRONLY:
		userAccess = BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermWriteOwner|quantumfs.PermWriteGroup|
				quantumfs.PermWriteOwner)
	case syscall.O_RDWR:
		userAccess = BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermWriteOther|quantumfs.PermWriteGroup|
				quantumfs.PermWriteOwner|quantumfs.PermReadOther|
				quantumfs.PermReadGroup|quantumfs.PermReadOwner)
	}

	var execAccess bool
	if BitFlagsSet(flags, FMODE_EXEC) {
		execAccess = BitAnyFlagSet(uint(record.Permissions()),
			quantumfs.PermExecOther|quantumfs.PermExecGroup|
				quantumfs.PermExecOwner|quantumfs.PermSUID|
				quantumfs.PermSGID)
	}

	success := userAccess || execAccess
	c.vlog("Permission check result %d", success)
	return success
}
