// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the File type, which represents regular files

import "errors"
import "sync"
import "syscall"

import "github.com/aristanetworks/quantumfs"

import "github.com/hanwen/go-fuse/fuse"

const FMODE_EXEC = 0x20 // From Linux

func newSmallFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId) {

	accessor := newSmallAccessor(c, size, key)

	return newFile_(c, name, inodeNum, key, parent, accessor), nil
}

func newMediumFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId) {

	accessor := newMediumAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor), nil
}

func newLargeFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId) {

	accessor := newLargeAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor), nil
}

func newVeryLargeFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId) {

	accessor := newVeryLargeAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor), nil
}

func newFile_(c *ctx, name string, inodeNum InodeId,
	key quantumfs.ObjectKey, parent Inode, accessor blockAccessor) *File {

	file := File{
		InodeCommon: InodeCommon{
			id:        inodeNum,
			name_:     name,
			accessed_: 0,
			treeLock_: parent.treeLock(),
		},
		accessor: accessor,
	}
	file.self = &file
	file.setParent(parent.inodeNum())

	assert(file.treeLock() != nil, "File treeLock nil at init")

	return &file
}

type File struct {
	InodeCommon
	accessor     blockAccessor
	unlinkRecord DirectoryRecordIf
}

// Mark this file dirty and notify your paent
func (fi *File) dirty(c *ctx) {
	defer c.funcIn("File::dirty").out()

	fi.setDirty(true)
	fi.parent(c).dirtyChild(c, fi.inodeNum())
}

func (fi *File) dirtyChild(c *ctx, child InodeId) {
	if child != fi.inodeNum() {
		panic("Unsupported dirtyChild() call on File")
	}
}

func (fi *File) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	c.elog("Unsupported Access on File")
	return fuse.ENOSYS
}

func (fi *File) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("File::GetAttr").out()

	record, err := fi.parent(c).getChildRecord(c, fi.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", fi.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, fi.InodeCommon.id, c.fuseCtx.Owner,
		record)

	return fuse.OK
}

func (fi *File) OpenDir(c *ctx, flags_ uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOTDIR
}

func (fi *File) openPermission(c *ctx, flags_ uint32) bool {
	defer c.funcIn("File::openPermission").out()

	record, error := fi.parent(c).getChildRecord(c, fi.id)
	if error != nil {
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

func (fi *File) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("File::Open").out()

	if !fi.openPermission(c, flags) {
		return fuse.EPERM
	}
	fi.self.markSelfAccessed(c, false)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(fi, fi.id, fileHandleNum, fi.treeLock())
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	c.dlog("Opened Inode %d as Fh: %d", fi.inodeNum(), fileHandleNum)

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

	defer c.funcIn("File::SetAttr").out()
	c.vlog("SetAttr valid %x size %d", attr.Valid, attr.Size)

	var updateMtime bool

	result := func() fuse.Status {
		defer fi.Lock().Unlock()

		c.vlog("Got file lock")

		if BitFlagsSet(uint(attr.Valid), fuse.FATTR_SIZE) {
			if attr.Size != fi.accessor.fileLength() {
				updateMtime = true
			}

			if attr.Size == 0 {
				fi.accessor.truncate(c, 0)
				return fuse.OK
			}
			endBlkIdx, _ := fi.accessor.blockIdxInfo(c, attr.Size-1)

			err := fi.reconcileFileType(c, endBlkIdx)
			if err != nil {
				c.elog("Could not reconcile file type with new end" +
					" blockIdx")
				return fuse.EIO
			}

			err = fi.accessor.truncate(c, uint64(attr.Size))
			if err != nil {
				return fuse.EIO
			}

			fi.self.dirty(c)
		}

		return fuse.OK
	}()

	if result != fuse.OK {
		return result
	}

	return fi.parent(c).setChildAttr(c, fi.InodeCommon.id, nil, attr, out,
		updateMtime)
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

func (fi *File) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on File")
	return fuse.ENOTDIR
}

func (fi *File) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on File")
	return nil, fuse.EINVAL
}

func (fi *File) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on File")
	return fuse.ENOSYS
}

func (fi *File) RenameChild(c *ctx, oldName string, newName string) fuse.Status {

	c.elog("Invalid RenameChild on File")
	return fuse.ENOSYS
}

func (fi *File) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on File")
	return fuse.ENOSYS
}

func (fi *File) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("File::GetXAttrSize").out()

	return fi.parent(c).getChildXAttrSize(c, fi.inodeNum(), attr)
}

func (fi *File) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("File::GetXAttrData").out()

	return fi.parent(c).getChildXAttrData(c, fi.inodeNum(), attr)
}

func (fi *File) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("File::ListXAttr").out()

	return fi.parent(c).listChildXAttr(c, fi.inodeNum())
}

func (fi *File) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.funcIn("File::SetXAttr").out()

	return fi.parent(c).setChildXAttr(c, fi.inodeNum(), attr, data)
}

func (fi *File) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.funcIn("File::RemoveXAttr").out()

	return fi.parent(c).removeChildXAttr(c, fi.inodeNum(), attr)
}

func (fi *File) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on File")
	return nil, nil
}

func (fi *File) syncChild(c *ctx, inodeNum InodeId, newKey quantumfs.ObjectKey) {
	c.elog("Invalid syncChild on File")
}

// When a file is unlinked its parent forgets about it, so we cannot ask it for our
// properties. Since the file cannot be accessed from the directory tree any longer
// we do not need to upload it or any of its content. When being unlinked we'll
// orphan the File by making it its own parent.
//
// Sometimes a File will access its own parent with or without its lock held. To
// protect the internal DirectoryRecord we'll abuse the InodeCommon.parentLock.
func (fi *File) setChildAttr(c *ctx, inodeNum InodeId, newType *quantumfs.ObjectType,
	attr *fuse.SetAttrIn, out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("File::setChildAttr").out()

	if inodeNum != fi.inodeNum() {
		c.elog("Invalid setChildAttr on File")
		return fuse.ENOSYS
	}

	c.dlog("File::setChildAttr Enter")
	defer c.dlog("File::setChildAttr Exit")
	fi.parentLock.Lock()
	defer fi.parentLock.Unlock()

	if fi.unlinkRecord == nil {
		panic("setChildAttr on self file before unlinking")
	}

	modifyEntryWithAttr(c, newType, attr, fi.unlinkRecord, updateMtime)

	if out != nil {
		fillAttrOutCacheData(c, out)
		fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
			c.fuseCtx.Owner, fi.unlinkRecord)
	}

	return fuse.OK
}

func (fi *File) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on File")
	return 0, fuse.ENODATA
}

func (fi *File) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on File")
	return nil, fuse.ENODATA
}

func (fi *File) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on File")
	return []byte{}, fuse.OK
}

func (fi *File) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on File")
	return fuse.Status(syscall.ENOSPC)
}

func (fi *File) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on File")
	return fuse.ENODATA
}

func (fi *File) getChildRecord(c *ctx, inodeNum InodeId) (DirectoryRecordIf, error) {

	defer c.funcIn("File::getChildRecord").out()

	if inodeNum != fi.inodeNum() {
		c.elog("Unsupported record fetch on file")
		return &quantumfs.DirectoryRecord{},
			errors.New("Unsupported record fetch")
	}

	c.dlog("File::getChildRecord Enter")
	defer c.dlog("File::getChildRecord Exit")
	fi.parentLock.Lock()
	defer fi.parentLock.Unlock()

	if fi.unlinkRecord == nil {
		panic("getChildRecord on self file before unlinking")
	}

	return fi.unlinkRecord, nil
}

func (fi *File) setChildRecord(c *ctx, record DirectoryRecordIf) {
	defer c.funcIn("File::setChildRecord").out()

	fi.parentLock.Lock()
	defer fi.parentLock.Unlock()

	if fi.unlinkRecord != nil {
		panic("setChildRecord on self file after unlinking")
	}

	fi.unlinkRecord = cloneDirectoryRecord(record)
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

func pushData(c *ctx, buffer quantumfs.Buffer) (quantumfs.ObjectKey, error) {
	key, err := buffer.Key(&c.Ctx)
	if err != nil {
		c.elog("Unable to write data to the datastore")
		return quantumfs.ObjectKey{},
			errors.New("Unable to write to the datastore")
	}

	return key, nil
}

func calcTypeGivenBlocks(numBlocks int) quantumfs.ObjectType {
	switch {
	case numBlocks <= 1:
		return quantumfs.ObjectTypeSmallFile
	case numBlocks <= quantumfs.MaxBlocksMediumFile():
		return quantumfs.ObjectTypeMediumFile
	case numBlocks <= quantumfs.MaxBlocksLargeFile():
		return quantumfs.ObjectTypeLargeFile
	default:
		return quantumfs.ObjectTypeVeryLargeFile
	}
}

// Given the block index to write into the file, ensure that we are the
// correct file type
func (fi *File) reconcileFileType(c *ctx, blockIdx int) error {
	defer c.funcIn("File::reconcileFileType").out()

	neededType := calcTypeGivenBlocks(blockIdx + 1)
	c.dlog("blockIdx %d", blockIdx)
	newAccessor := fi.accessor.convertTo(c, neededType)
	if newAccessor == nil {
		return errors.New("Unable to process needed type for accessor")
	}

	if fi.accessor != newAccessor {
		fi.accessor = newAccessor
		var attr fuse.SetAttrIn
		fi.parent(c).setChildAttr(c, fi.id, &neededType, &attr, nil, false)
	}
	return nil
}

type blockAccessor interface {

	// Read data from the block via an index
	readBlock(*ctx, int, uint64, []byte) (int, error)

	// Write data to a block via an index
	writeBlock(*ctx, int, uint64, []byte) (int, error)

	// Get the file's length in bytes
	fileLength() uint64

	// Extract block and remaining offset from absolute offset
	blockIdxInfo(c *ctx, absOffset uint64) (int, uint64)

	// Convert contents into new accessor type, nil accessor if current is fine
	convertTo(*ctx, quantumfs.ObjectType) blockAccessor

	// Write file's metadata to the datastore and provide the key
	sync(c *ctx) quantumfs.ObjectKey

	// Truncate to lessen length *only*, error otherwise
	truncate(c *ctx, newLength uint64) error
}

func (fi *File) writeBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	defer c.funcIn("File::writeBlock").out()

	err := fi.reconcileFileType(c, blockIdx)
	if err != nil {
		c.elog("Could not reconcile file type with new blockIdx")
		return 0, err
	}

	var written int
	written, err = fi.accessor.writeBlock(c, blockIdx, offset, buf)
	if err != nil {
		return 0, err
	}

	return written, nil
}

type blockFn func(*ctx, int, uint64, []byte) (int, error)

// Returns the number of bytes operated on, and any error code
func (fi *File) operateOnBlocks(c *ctx, offset uint64, size uint32, buf []byte,
	fn blockFn) (uint64, error) {

	defer c.funcIn("File::operateOnBlocks").out()
	c.vlog("operateOnBlocks offset %d size %d", offset, size)

	count := uint64(0)

	// Ensure size and buf are consistent
	buf = buf[:size]
	size = uint32(len(buf))

	if size == 0 {
		c.vlog("block operation with zero size or buf")
		return 0, nil
	}

	// Determine the block to start in
	startBlkIdx, newOffset := fi.accessor.blockIdxInfo(c, offset)
	endBlkIdx, _ := fi.accessor.blockIdxInfo(c, offset+uint64(size)-1)
	offset = newOffset

	// Handle the first block a little specially (with offset)
	c.dlog("Reading initial block %d offset %d", startBlkIdx, offset)
	iterCount, err := fn(c, startBlkIdx, offset, buf[count:])
	if err != nil {
		c.elog("Unable to operate on first data block")
		return 0, errors.New("Unable to operate on first data block")
	}
	count += uint64(iterCount)

	c.vlog("Processing blocks %d to %d", startBlkIdx+1, endBlkIdx)
	// Loop through the blocks, operating on them
	for i := startBlkIdx + 1; i <= endBlkIdx; i++ {
		iterCount, err = fn(c, i, 0, buf[count:])
		if err != nil {
			// We couldn't do more, but that's okay we've done some
			// already so just return early and report what we've done
			break
		}
		count += uint64(iterCount)
	}

	return count, nil
}

func (fi *File) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.funcIn("File::Read").out()
	defer fi.Lock().Unlock()

	readCount, err := fi.operateOnBlocks(c, offset, size, buf,
		fi.accessor.readBlock)

	if err != nil {
		return fuse.ReadResult(nil), fuse.EIO
	}

	return fuse.ReadResultData(buf[:readCount]), fuse.OK
}

func (fi *File) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	defer c.funcIn("File::Write").out()
	c.vlog("offset %d size %d flags %x", offset, size, flags)

	writeCount, result := func() (uint32, fuse.Status) {
		defer fi.Lock().Unlock()

		writeCount, err := fi.operateOnBlocks(c, offset, size, buf,
			fi.writeBlock)

		if err != nil {
			return 0, fuse.EIO
		}
		fi.self.dirty(c)
		return uint32(writeCount), fuse.OK
	}()

	if result != fuse.OK {
		return writeCount, result
	}

	// Update the size with what we were able to write
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = uint64(fi.accessor.fileLength())
	fi.parent(c).setChildAttr(c, fi.id, nil, &attr, nil, true)
	fi.dirty(c)

	return writeCount, fuse.OK
}

func newFileDescriptor(file *File, inodeNum InodeId,
	fileHandleId FileHandleId, treeLock *sync.RWMutex) FileHandle {

	fd := &FileDescriptor{
		FileHandleCommon: FileHandleCommon{
			id:        fileHandleId,
			inodeNum:  inodeNum,
			treeLock_: treeLock,
		},
		file: file,
	}

	assert(fd.treeLock() != nil, "FileDescriptor treeLock nil at init")
	return fd
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
