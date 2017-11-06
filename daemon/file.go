// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the File type, which represents regular files

import (
	"errors"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

const FMODE_EXEC = 0x20 // From Linux

func newSmallFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newSmallFile", "name %s", name).Out()

	accessor := newSmallAccessor(c, size, key)

	return newFile_(c, name, inodeNum, key, parent, accessor,
		quantumfs.ObjectTypeSmallFile), nil
}

func newMediumFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newMediumFile", "name %s", name).Out()

	accessor := newMediumAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor,
		quantumfs.ObjectTypeMediumFile), nil
}

func newLargeFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newLargeFile", "name %s", name).Out()

	accessor := newLargeAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor,
		quantumfs.ObjectTypeLargeFile), nil
}

func newVeryLargeFile(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newVeryLargeFile", "name %s", name).Out()

	accessor := newVeryLargeAccessor(c, key)

	return newFile_(c, name, inodeNum, key, parent, accessor,
		quantumfs.ObjectTypeVeryLargeFile), nil
}

func newFile_(c *ctx, name string, inodeNum InodeId,
	key quantumfs.ObjectKey, parent Inode, accessor blockAccessor,
	accessorType quantumfs.ObjectType) *File {

	defer c.funcIn("newFile_").Out()

	file := File{
		InodeCommon: InodeCommon{
			id:        inodeNum,
			name_:     name,
			accessed_: 0,
			treeLock_: parent.treeLock(),
		},
		accessor:     accessor,
		accessorType: accessorType,
	}
	file.self = &file
	file.setParent(parent.inodeNum())

	utils.Assert(file.treeLock() != nil, "File treeLock nil at init")

	return &file
}

type File struct {
	InodeCommon
	accessor     blockAccessor
	accessorType quantumfs.ObjectType
}

func (fi *File) handleAccessorTypeChange(c *ctx,
	remoteRecord quantumfs.DirectoryRecord) {

	defer c.FuncIn("File::handleAccessorTypeChange", "%s: %d",
		remoteRecord.Filename(), remoteRecord.Type()).Out()
	switch remoteRecord.Type() {
	case quantumfs.ObjectTypeSmallFile:
		fi.accessor = newSmallAccessor(c, 0, remoteRecord.ID())
		fi.accessor.reload(c, remoteRecord.ID())
	case quantumfs.ObjectTypeMediumFile:
		fi.accessor = newMediumAccessor(c, remoteRecord.ID())
	case quantumfs.ObjectTypeLargeFile:
		fi.accessor = newLargeAccessor(c, remoteRecord.ID())
	case quantumfs.ObjectTypeVeryLargeFile:
		fi.accessor = newVeryLargeAccessor(c, remoteRecord.ID())
	}
}

func (fi *File) dirtyChild(c *ctx, child InodeId) {
	c.FuncIn("FuncIn::dirtyChild", "inode %d", child).Out()
	if child != fi.inodeNum() {
		panic("Unsupported dirtyChild() call on File")
	}
}

func (fi *File) Access(c *ctx, mask uint32, uid uint32, gid uint32) fuse.Status {

	defer c.funcIn("File::Access").Out()
	return hasAccessPermission(c, fi, mask, uid, gid)
}

func (fi *File) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("File::GetAttr").Out()

	record, err := fi.parentGetChildRecordCopy(c, fi.InodeCommon.id)
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

	c.vlog("File::OpenDir doing nothing")
	return fuse.ENOTDIR
}

func (fi *File) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("File::Open").Out()

	err := hasPermissionOpenFlags(c, fi, flags)
	if err != fuse.OK {
		return err
	}

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(fi, fi.id, fileHandleNum, fi.treeLock())
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	c.dlog("Opened Inode %d as Fh: %d", fi.inodeNum(), fileHandleNum)

	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	out.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (fi *File) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.vlog("File::Lookup doing nothing")
	return fuse.ENOSYS
}

func (fi *File) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("File::Create doing nothing")
	return fuse.ENOTDIR
}

func (fi *File) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.funcIn("File::SetAttr").Out()

	var updateMtime bool

	if utils.BitFlagsSet(uint(attr.Valid), fuse.FATTR_SIZE) {
		result := func() fuse.Status {
			defer fi.Lock().Unlock()

			c.vlog("Got file lock")

			if attr.Size != fi.accessor.fileLength(c) {
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

			return fuse.OK
		}()

		if result != fuse.OK {
			return result
		}

		fi.self.markSelfAccessed(c, quantumfs.PathUpdated)
	}

	return fi.parentSetChildAttr(c, fi.InodeCommon.id, attr, out, updateMtime)
}

func (fi *File) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("File::Mkdir doing nothing")
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

	defer c.funcIn("File::GetXAttrSize").Out()

	return fi.parentGetChildXAttrSize(c, fi.inodeNum(), attr)
}

func (fi *File) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("File::GetXAttrData").Out()

	return fi.parentGetChildXAttrData(c, fi.inodeNum(), attr)
}

func (fi *File) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("File::ListXAttr").Out()

	return fi.parentListChildXAttr(c, fi.inodeNum())
}

func (fi *File) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.FuncIn("File::SetXAttr", "%s", attr).Out()

	return fi.parentSetChildXAttr(c, fi.inodeNum(), attr, data)
}

func (fi *File) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.funcIn("File::RemoveXAttr").Out()

	return fi.parentRemoveChildXAttr(c, fi.inodeNum(), attr)
}

func (fi *File) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on File")
	return nil, nil
}

func (fi *File) syncChild(c *ctx, inodeNum InodeId, newKey quantumfs.ObjectKey,
	newType quantumfs.ObjectType) {

	c.elog("Invalid syncChild on File")
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
	defer c.funcIn("File::reconcileFileType").Out()

	neededType := calcTypeGivenBlocks(blockIdx + 1)
	c.dlog("blockIdx %d", blockIdx)
	newAccessor := fi.accessor.convertTo(c, neededType)
	if newAccessor == nil {
		return errors.New("Unable to process needed type for accessor")
	}

	if fi.accessor != newAccessor {
		fi.accessor = newAccessor
		fi.accessorType = neededType
	}
	return nil
}

type blockAccessor interface {

	// Read data from the block via an index
	readBlock(*ctx, int, uint64, []byte) (int, error)

	// Write data to a block via an index
	writeBlock(*ctx, int, uint64, []byte) (int, error)

	// Get the file's length in bytes
	fileLength(c *ctx) uint64

	// Extract block and remaining offset from absolute offset
	blockIdxInfo(c *ctx, absOffset uint64) (int, uint64)

	// Convert contents into new accessor type, nil accessor if current is fine
	convertTo(*ctx, quantumfs.ObjectType) blockAccessor

	// Write file's metadata to the datastore and provide the key
	sync(c *ctx) quantumfs.ObjectKey

	// Reload the content of the file from datastore
	reload(c *ctx, key quantumfs.ObjectKey)

	// Truncate to lessen length *only*, error otherwise
	truncate(c *ctx, newLength uint64) error
}

func (fi *File) writeBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	defer c.funcIn("File::writeBlock").Out()

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

type blockFn func(*ctx, int, uint64) error

func operateOnBlocks(c *ctx, accessor blockAccessor, offset uint64, size uint32,
	fn blockFn) error {

	defer c.funcIn("File::operateOnBlocks").Out()
	c.vlog("operateOnBlocks offset %d size %d", offset, size)

	if size == 0 {
		c.vlog("block operation with zero size or buf")
		return nil
	}

	// Determine the block to start in
	startBlkIdx, newOffset := accessor.blockIdxInfo(c, offset)
	endBlkIdx, _ := accessor.blockIdxInfo(c, offset+uint64(size)-1)
	offset = newOffset

	// Handle the first block a little specially (with offset)
	c.dlog("Reading initial block %d offset %d", startBlkIdx, offset)
	err := fn(c, startBlkIdx, offset)
	if err != nil {
		c.elog("Unable to operate on first data block: %s", err.Error())
		return errors.New("Unable to operate on first data block")
	}

	c.vlog("Processing blocks %d to %d", startBlkIdx+1, endBlkIdx)
	// Loop through the blocks, operating on them
	for i := startBlkIdx + 1; i <= endBlkIdx; i++ {
		err = fn(c, i, 0)
		if err != nil {
			// We couldn't do more, but that's okay we've done some
			// already so just return early and report what we've done
			c.elog("Block operation stopped early: %s", err.Error())
			break
		}
	}

	return nil
}

func (fi *File) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.funcIn("File::Read").Out()

	var readCount int
	readResult, status := func() (fuse.ReadResult, fuse.Status) {
		defer fi.Lock().Unlock()

		// Ensure size and buf are consistent
		buf = buf[:size]
		size = uint32(len(buf))

		err := operateOnBlocks(c, fi.accessor, offset, size,
			func(c *ctx, blockIdx int, offset uint64) error {
				read, err := fi.accessor.readBlock(c, blockIdx,
					offset, buf[readCount:])

				readCount += read
				return err
			})

		if err != nil {
			return fuse.ReadResult(nil), fuse.EIO
		}

		return fuse.ReadResultData(buf[:readCount]), fuse.OK
	}()

	if status == fuse.OK {
		fi.self.markSelfAccessed(c, quantumfs.PathRead)
		c.vlog("Returning %d bytes", readCount)
	}

	return readResult, status
}

func (fi *File) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	defer c.funcIn("File::Write").Out()
	c.vlog("offset %d size %d flags %x", offset, size, flags)

	writeCount, result := func() (uint32, fuse.Status) {
		defer fi.Lock().Unlock()

		// Ensure size and buf are consistent
		buf = buf[:size]
		size = uint32(len(buf))

		writeCount_ := 0
		err := operateOnBlocks(c, fi.accessor, offset, size,
			func(c *ctx, blockIdx int, offset uint64) error {
				written, err := fi.writeBlock(c, blockIdx,
					offset, buf[writeCount_:])

				writeCount_ += written
				return err
			})

		if err != nil {
			return 0, fuse.EIO
		}
		fi.self.dirty(c)
		return uint32(writeCount_), fuse.OK
	}()

	if result != fuse.OK {
		return writeCount, result
	}

	// Update the size with what we were able to write
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = uint64(fi.accessor.fileLength(c))
	fi.parentSetChildAttr(c, fi.id, &attr, nil, true)
	fi.dirty(c)
	fi.self.markSelfAccessed(c, quantumfs.PathUpdated)

	return writeCount, fuse.OK
}

func (fi *File) flush(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("File::flush", "%s", fi.name_).Out()

	key := quantumfs.EmptyBlockKey
	fi.parentSyncChild(c, func() (quantumfs.ObjectKey, quantumfs.ObjectType) {
		key = fi.accessor.sync(c)
		return key, fi.accessorType
	})
	return key
}

func newFileDescriptor(file *File, inodeNum InodeId,
	fileHandleId FileHandleId, treeLock *TreeLock) FileHandle {

	fd := &FileDescriptor{
		FileHandleCommon: FileHandleCommon{
			id:        fileHandleId,
			inodeNum:  inodeNum,
			treeLock_: treeLock,
		},
		file: file,
	}

	utils.Assert(fd.treeLock() != nil, "FileDescriptor treeLock nil at init")
	return fd
}

type FileDescriptor struct {
	FileHandleCommon
	file *File
}

func (fd *FileDescriptor) dirty(c *ctx) {
	defer c.funcIn("FileDescriptor::dirty").Out()
	fd.file.self.dirty(c)
}

func (fd *FileDescriptor) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against FileDescriptor")
	return fuse.ENOSYS
}

func (fd *FileDescriptor) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.funcIn("FileDescriptor::Read").Out()

	return fd.file.Read(c, offset, size, buf, nonblocking)
}

func (fd *FileDescriptor) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	defer c.funcIn("FileDescriptor::Write").Out()

	return fd.file.Write(c, offset, size, flags, buf)
}
