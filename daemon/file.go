// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the File type, which represents regular files

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"
import "crypto/sha1"

func newFile(inodeNum InodeId, fileType quantumfs.ObjectType,
	key quantumfs.ObjectKey, parent Inode) *File {

	file := File{
		InodeCommon: InodeCommon{id: inodeNum},
		fileType:    fileType,
		key:         key,
		parent:      parent,
	}
	file.self = &file
	return &file
}

type File struct {
	InodeCommon
	fileType quantumfs.ObjectType
	key      quantumfs.ObjectKey
	parent   Inode
}

// Mark this file dirty and notify your paent
func (fi *File) dirty(c *ctx) {
	fi.dirty_ = true
	fi.parent.dirtyChild(c, fi)
}

func (fi *File) sync(c *ctx) quantumfs.ObjectKey {
	fi.dirty_ = false
	return fi.key
}

func (fi *File) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	c.elog("Unsupported Access on File")
	return fuse.ENOSYS
}

func (fi *File) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {

	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillAttrWithDirectoryRecord(c, &out.Attr, fi.InodeCommon.id, c.fuseCtx.Owner,
		wsr.childrenRecords[inodeNum])

	return fuse.OK
}

func (fi *File) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(fi, fi.id, fileHandleNum)
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	out.OpenFlags = 0
	out.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (fi *File) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {

	return fuse.ENOSYS
}

func (fi *File) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	inodeNum := fi.InodeCommmon.id
	entry, exists := wsr.childrenRecords[inodeNum]
	if !exists {
		return fuse.ENOENT
	}

	valid := uint(attr.SetAttrInCommon.Valid)
	if BitFlagsSet(valid, fuse.FATTR_FH|
		fuse.FATTR_LOCKOWNER) {
		c.elog("Unsupported attribute(s) to set", valid)
		return fuse.ENOSYS
	}

	if BitFlagsSet(valid, fuse.FATTR_MODE) {
		entry.Permissions = modeToPermissions(attr.Mode, 0)
	}

	if BitFlagsSet(valid, fuse.FATTR_UID) {
		entry.Owner = quantumfs.ObjectUid(c.Ctx, attr.Owner.Uid,
			c.fuseCtx.Owner.Uid)
	}

	if BitFlagsSet(valid, fuse.FATTR_GID) {
		entry.Group = quantumfs.ObjectGid(c.Ctx, attr.Owner.Gid,
			c.fuseCtx.Owner.Gid)
	}

	if BitFlagsSet(valid, fuse.FATTR_SIZE) {
		entry.Size = attr.Size
	}

	if BitFlagsSet(valid, fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) {
		// atime is ignored and not stored
	}

	if BitFlagsSet(valid, fuse.FATTR_MTIME) {
		entry.ModificationTime = quantumfs.NewTimeSeconds(attr.Mtime,
			attr.Mtimensec)
	}

	if BitFlagsSet(valid, fuse.FATTR_MTIME_NOW) {
		entry.ModificationTime = quantumfs.NewTime(time.Now())
	}

	if BitFlagsSet(valid, fuse.FATTR_CTIME) {
		entry.CreationTime = quantumfs.NewTimeSeconds(attr.Ctime,
			attr.Ctimensec)
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
		c.fuseCtx.Owner, entry)

	wsr.dirty(c)

	return fuse.OK
}

func (fi *File) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	if fi.key == quantumfs.EmptyBlockKey {
		return fuse.ReadResultData(nil), fuse.OK
	}

	data := DataStore.Get(c, fi.key)
	curData := data.Get()

	end := offset + uint64(len(buf))
	if end > uint64(len(curData)) {
		end = uint64(len(curData))
	}

	copied := copy(buf, curData[offset:end])

	return fuse.ReadResultData(buf[0:copied]), fuse.OK
}

func (fi *File) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	var finalData *quantumfs.Buffer

	if fi.key != quantumfs.EmptyBlockKey {
		data := DataStore.Get(c, fi.key)
		if data == nil {
			c.elog("Data for key missing from datastore")
			return 0, fuse.EIO
		}
		finalData = quantumfs.NewBuffer(data.Get())
	} else {
		finalData = quantumfs.NewBuffer([]byte{})
	}

	if offset > uint64(len(finalData.Get())) {
		c.elog("Writing past the end of file is not supported yet")
		return 0, fuse.EIO
	}
	if size > uint32(len(buf)) {
		size = uint32(len(buf))
	}

	copied := finalData.Write(buf[:size], uint32(offset))
	if copied > 0 {
		hash := sha1.Sum(finalData.Get())
		newFileKey := quantumfs.NewObjectKey(quantumfs.KeyTypeData, hash)

		err := DataStore.Set(c, newFileKey,
			quantumfs.NewBuffer(finalData.Get()))
		if err != nil {
			c.elog("Unable to write data to the datastore")
			return 0, fuse.EIO
		}

		fi.key = newFileKey
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = uint64(len(finalData.Get()))
		fillAttrOutCacheData(c, out)
		fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
			entry)
		fi.dirty(c)
	}

	return copied, fuse.OK
}

func newFileDescriptor(file *File, inodeNum InodeId,
	fileHandleId FileHandleId) FileHandle {

	return &FileDescriptor{
		FileHandleCommon: FileHandleCommon{
			id:       fileHandleId,
			inodeNum: inodeNum,
		},
		file: file,
	}
}

type FileDescriptor struct {
	FileHandleCommon
	file *File
}

func (fd *FileDescriptor) dirty(c *ctx) {
	fd.file.self.dirty(c)
}

func (fd *FileDescriptor) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against FileDescriptor")
	return fuse.ENOSYS
}

func (fd *FileDescriptor) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	return fd.file.Read(c, offset, size, buf, nonblocking)
}

func (fd *FileDescriptor) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	return fd.file.Write(c, offset, size, flags, buf)
}
