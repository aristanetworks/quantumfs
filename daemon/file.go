// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the File type, which represents regular files

import "arista.com/quantumfs"
import "errors"
import "github.com/hanwen/go-fuse/fuse"
import "crypto/sha1"
import "syscall"

const execBit = 0x1
const writeBit = 0x2
const readBit = 0x4

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

func enforceSize(buffer []byte, size int) []byte {
	if len(buffer) > size {
		return buffer[:size]
	}

	//if the size requested is larger than the buffer, we don't change the buffer
	return buffer
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
	record, err := fi.parent.getChildRecord(c, fi.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", fi.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, fi.InodeCommon.id, c.fuseCtx.Owner,
		&record)

	return fuse.OK
}

func (fi *File) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) openPermission(c *ctx, flags uint32) bool {
	record, error := fi.parent.getChildRecord(c, fi.id)
	if error != nil {
		return false
	}

	c.vlog("Open permission check. Have %x, flags %x", record.Permissions, flags)
	//this only works because we don't have owner/group/other specific perms.
	//we need to confirm whether we can treat the root user/group specially.
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		return (record.Permissions & readBit) != 0
	case syscall.O_WRONLY:
		return (record.Permissions & writeBit) != 0
	case syscall.O_RDWR:
		var bitmask uint8 = readBit | writeBit
		return (record.Permissions & bitmask) == bitmask
	}

	return false
}

func (fi *File) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	if !fi.openPermission(c, flags) {
		return fuse.EPERM
	}

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

	record, err := fi.parent.getChildRecord(c, fi.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", fi.id)
		return fuse.EIO
	}

	// If we're truncating the attr.Size, truncate the data BEFORE we mod attr
	if BitFlagsSet(uint(attr.Valid), fuse.FATTR_SIZE) &&
		record.Size > attr.Size {

		curBuffer := fi.fetchData(c)
		if curBuffer == nil {
			c.elog("Unable to fetch existing data for file")
			return fuse.EIO
		}

		curBuffer.Set(enforceSize(curBuffer.Get(), int(attr.Size)))

		err = fi.pushData(c, curBuffer)
		if err != nil {
			c.elog("Unable to truncate file data: %v", err)
			return fuse.EIO
		}
	}

	return fi.parent.setChildAttr(c, fi.InodeCommon.id, attr, out)
}

func (fi *File) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on File")
	return fuse.ENOTDIR
}

func (fi *File) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on File")
	return fuse.ENOTDIR
}

func (fi *File) setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid setChildAttr on File")
	return fuse.ENOSYS
}

func (fi *File) getChildRecord(c *ctx, inodeNum InodeId) (quantumfs.DirectoryRecord,
	error) {

	c.elog("Unsupported record fetch on file")
	return quantumfs.DirectoryRecord{}, errors.New("Unsupported record fetch")
}

func (fi *File) fetchData(c *ctx) *quantumfs.Buffer {
	if fi.key != quantumfs.EmptyBlockKey {
		data := DataStore.Get(c, fi.key)
		if data == nil {
			c.elog("Data for key missing from datastore")
			return nil
		}
		return quantumfs.NewBuffer(data.Get())
	}

	return quantumfs.NewBuffer([]byte{})
}

func (fi *File) pushData(c *ctx, buffer *quantumfs.Buffer) error {
	hash := sha1.Sum(buffer.Get())
	newFileKey := quantumfs.NewObjectKey(quantumfs.KeyTypeData, hash)

	err := DataStore.Set(c, newFileKey,
		quantumfs.NewBuffer(buffer.Get()))
	if err != nil {
		c.elog("Unable to write data to the datastore")
		return errors.New("Unable to write to the datastore")
	}

	fi.key = newFileKey
	return nil
}

func (fi *File) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	// Get the record for this file so we can enforce its attributes on the data
	record, err := fi.parent.getChildRecord(c, fi.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", fi.id)
		return fuse.ReadResultData(nil), fuse.EIO
	}

	curBuffer := fi.fetchData(c)
	if curBuffer == nil {
		c.elog("Unable to fetch existing data for file")
		return fuse.ReadResultData(nil), fuse.EIO
	}

	curData := enforceSize(curBuffer.Get(), int(record.Size))

	end := offset + uint64(len(buf))
	if end > uint64(len(curData)) {
		end = uint64(len(curData))
	}

	copied := copy(buf, curData[offset:end])

	return fuse.ReadResultData(buf[0:copied]), fuse.OK
}

func (fi *File) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	// Get the record for this file so we can enforce its attributes on the data
	record, err := fi.parent.getChildRecord(c, fi.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", fi.id)
		return 0, fuse.EIO
	}

	finalData := fi.fetchData(c)
	if finalData == nil {
		c.elog("Unable to fetch existing data for file")
		return 0, fuse.EIO
	}

	finalData.Set(enforceSize(finalData.Get(), int(record.Size)))

	if offset > uint64(len(finalData.Get())) {
		offset = uint64(len(finalData.Get()))
	}
	if size > uint32(len(buf)) {
		size = uint32(len(buf))
	}

	copied := finalData.Write(buf[:size], uint32(offset))
	if copied > 0 {
		err = fi.pushData(c, finalData)
		if err != nil {
			c.elog("Write failed")
			return 0, fuse.EIO
		}

		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		// Don't overwrite the size, increment it only to preserve holes
		attr.Size = record.Size + uint64(copied)
		fi.parent.setChildAttr(c, fi.id, &attr, nil)
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
