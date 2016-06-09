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
	file.accessor = &SmallFile{
		key:	key,
		bytes:	0,
	}

	return &file
}

type File struct {
	InodeCommon
	fileType quantumfs.ObjectType
	key      quantumfs.ObjectKey
	parent   Inode
	accessor BlockAccessor
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

	if BitFlagsSet(uint(attr.Valid), fuse.FATTR_SIZE) {
		err := fi.accessor.Truncate(c, uint32(attr.Size))
		if err != nil {
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

func resize(buffer []byte, size int) []byte {
	if len(buffer) > size {
		return buffer[:size]
	}

	for len(buffer) < size {
		newLength := make([]byte, size-len(buffer))
		buffer = append(buffer, newLength...)
	}

	return buffer
}

func fetchData(c *ctx, key quantumfs.ObjectKey) *quantumfs.Buffer {
	var rtn *quantumfs.Buffer

	if key != quantumfs.EmptyBlockKey {
		data := DataStore.Get(c, key)
		if data == nil {
			c.elog("Data for key missing from datastore")
			return nil
		}
		rtn = quantumfs.NewBuffer(data.Get())
	} else {
		rtn = quantumfs.NewBuffer([]byte{})
	}

	return rtn
}

func fetchDataSized(c *ctx, key quantumfs.ObjectKey,
	targetSize int) *quantumfs.Buffer { 

	rtn := fetchData(c, key)

	// Before we return the buffer, make sure it's the size it needs to be
	rtn.Set(resize(rtn.Get(), targetSize))

	return rtn
}

func pushData(c *ctx, buffer *quantumfs.Buffer) (*quantumfs.ObjectKey, error) {

	hash := sha1.Sum(buffer.Get())
	newFileKey := quantumfs.NewObjectKey(quantumfs.KeyTypeData, hash)

	err := DataStore.Set(c, newFileKey,
		quantumfs.NewBuffer(buffer.Get()))
	if err != nil {
		c.elog("Unable to write data to the datastore")
		return nil, errors.New("Unable to write to the datastore")
	}

	return &newFileKey, nil
}

func calcTypeGivenBlocks(numBlocks int) quantumfs.ObjectType {
	if numBlocks <= 1 {
		return quantumfs.ObjectTypeSmallFile
	}

	if numBlocks <= quantumfs.MaxBlocksMediumFile {
		return quantumfs.ObjectTypeMediumFile
	}

	if numBlocks <= quantumfs.MaxBlocksLargeFile {
		return quantumfs.ObjectTypeLargeFile
	}

	return quantumfs.ObjectTypeVeryLargeFile
}

type BlockAccessor interface {

	// Read data from the block via an index
	ReadBlock(*ctx, int, uint64, []byte) (int, error)

	// Write data to a block via an index
	WriteBlock(*ctx, int, uint64, []byte) (int, error)

	// Get the file's length in bytes
	GetFileLength() uint64

	// Get the file's accessor type
	GetType() quantumfs.ObjectType

	// Get the length of a block for this file
	GetBlockLength() uint64

	// Convert contents into new accessor type
	ConvertTo(*ctx, quantumfs.ObjectType) BlockAccessor

	// Get the file's direct data in bytes (raw or metadata depending on type)
	Marshal() ([]byte, error)

	// Truncate to lessen length *only*, error otherwise
	Truncate(*ctx, uint32) error
}

// Given the number of blocks to write into the file, ensure that we are the
// correct file type
func (fi *File) reconcileFileType(c *ctx, numBlocks int) error {

	neededType := calcTypeGivenBlocks(numBlocks)
	if neededType > fi.accessor.GetType() {
		newAccessor := fi.accessor.ConvertTo(c, neededType)
		if newAccessor == nil {
			return errors.New("Could not convert block accessor")
		}
		fi.accessor = newAccessor
	}
	return nil
}

func (fi *File) writeBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	err := fi.reconcileFileType(c, blockIdx)
	if err != nil {
		c.elog("Could not reconcile file type with new blockIdx")
		return 0, err
	}

	var written int
	written, err = fi.accessor.WriteBlock(c, blockIdx, offset, buf)
	if err != nil {
		return 0, errors.New("Couldn't write block")
	}

	return written, nil
}

func (fi *File) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	readCount := 0

	// Ensure size and buf are consistent
	buf = buf[:size]
	size = uint32(len(buf))

	if size == 0 {
		c.vlog("Read with zero size or buf")
		return fuse.ReadResult(nil), fuse.OK
	}

	// Determine the block to start in
	startBlkIdx := int(offset / fi.accessor.GetBlockLength())
	endBlkIdx := int((offset + uint64(size)) / fi.accessor.GetBlockLength())
	offset = offset % fi.accessor.GetBlockLength()

	// Read the first block a little specially (with offset)
	read, err := fi.accessor.ReadBlock(c, startBlkIdx, offset, buf[readCount:])
	if err != nil {
		c.elog("Unable to read first data block")
		return fuse.ReadResult(nil), fuse.EIO
	}
	readCount += read

	// Loop through the blocks, reading them
	for i := startBlkIdx + 1; i <= endBlkIdx; i++ {
		read, err = fi.accessor.ReadBlock(c, i, 0, buf[readCount:])
		if err != nil {
			// We couldn't read more, but that's okay we've read some
			// already so just return early and report what we've done
			break;
		}
		readCount += read
	}

	return fuse.ReadResultData(buf[:readCount]), fuse.OK
}

func (fi *File) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	writeCount := 0

	// Ensure size and buf are consistent
	buf = buf[:size]
	size = uint32(len(buf))

	if size == 0 {
		c.vlog("Write with zero size or buf")
		return 0, fuse.OK
	}

	// Determine the block to start in
	startBlkIdx := int(offset / fi.accessor.GetBlockLength())
	endBlkIdx := int((offset + uint64(size)) / fi.accessor.GetBlockLength())
	offset = offset % fi.accessor.GetBlockLength()

	// Write the first block a little specially (with offset)
	written, err := fi.writeBlock(c, startBlkIdx, offset,
		buf[writeCount:])
	if err != nil {
		c.elog("Unable to write first data block")
		return 0, fuse.EIO
	}
	writeCount += written

	// Loop through the blocks, writing them
	for i := startBlkIdx + 1; i <= endBlkIdx; i++ {
		written, err = fi.writeBlock(c, i, 0, buf[writeCount:])
		if err != nil {
			// We couldn't write more, but that's okay we've written some
			// already so just return early and report what we've done
			break;
		}
		writeCount += written
	}

	// Update the direct entry
	var bytes []byte
	bytes, err = fi.accessor.Marshal()
	if err != nil {
		panic("Failed to marshal baselayer")
	}
	hash := sha1.Sum(bytes)
	newBaseLayerId := quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata, hash)

	var buffer quantumfs.Buffer
	buffer.Set(bytes)
	if err := c.durableStore.Set(newBaseLayerId, &buffer); err != nil {
		panic("Failed to upload new baseLayer object")
	}

	// Update the size with what we were able to write	
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = uint64(fi.accessor.GetFileLength())
	fi.parent.setChildAttr(c, fi.id, &attr, nil)
	fi.dirty(c)

	return uint32(writeCount), fuse.OK
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
