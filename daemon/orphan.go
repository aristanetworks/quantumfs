// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"bytes"
	"errors"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// When a file is unlinked its parent forgets about it, so we cannot ask it for our
// properties. Since the file cannot be accessed from the directory tree any longer
// we do not need to upload it or any of its content. When being unlinked we'll
// orphan the Inode by making it its own parent.
func (inode *InodeCommon) setOrphanChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn, out *fuse.AttrOut,
	updateMtime bool) fuse.Status {

	defer c.funcIn("InodeCommon::setOrphanChildAttr").Out()
	defer inode.unlinkLock.Lock().Unlock()

	if inode.unlinkRecord == nil {
		panic("setChildAttr on self file before unlinking")
	}

	modifyEntryWithAttr(c, newType, attr, inode.unlinkRecord, updateMtime)

	if out != nil {
		fillAttrOutCacheData(c, out)
		fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
			c.fuseCtx.Owner, inode.unlinkRecord)
	}

	return fuse.OK
}

// Requires unlinkLock
func (inode *InodeCommon) parseExtendedAttributes_(c *ctx) {
	defer c.funcIn("InodeCommon::parseExtendedAttributes_").Out()

	if inode.unlinkXAttr != nil {
		return
	}

	// Download and parse the extended attributes
	inode.unlinkXAttr = make(map[string][]byte)

	key := inode.unlinkRecord.ExtendedAttributes()
	if key.IsEqualTo(quantumfs.EmptyBlockKey) {
		return
	}

	buffer := c.dataStore.Get(&c.Ctx, key)
	if buffer == nil {
		c.elog("Failed to retrieve extended attribute list")
		return
	}

	attributes := buffer.AsExtendedAttributes()

	for i := 0; i < attributes.NumAttributes(); i++ {
		name, attrKey := attributes.Attribute(i)

		c.vlog("Found attribute key: %s", attrKey.String())
		buffer := c.dataStore.Get(&c.Ctx, attrKey)
		if buffer == nil {
			c.elog("Failed to retrieve attribute datablock")
			continue
		}

		inode.unlinkXAttr[name] = buffer.Get()
	}
}

func (inode *InodeCommon) getExtendedAttribute(c *ctx, attr string) ([]byte, bool) {
	defer c.FuncIn("InodeCommon::getExtendedAttribute", "Attr: %s", attr).Out()

	defer inode.unlinkLock.Lock().Unlock()

	inode.parseExtendedAttributes_(c)

	data, ok := inode.unlinkXAttr[attr]
	return data, ok
}

func (inode *InodeCommon) getOrphanChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("InodeCommon::getOrphanChildXAttrSize").Out()
	value, ok := inode.getExtendedAttribute(c, attr)
	if !ok {
		// No such attribute
		return 0, fuse.ENODATA
	} else {
		return len(value), fuse.OK
	}
}

func (inode *InodeCommon) getOrphanChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::getOrphanChildXAttrData").Out()
	value, ok := inode.getExtendedAttribute(c, attr)
	if !ok {
		// No such attribute
		return nil, fuse.ENODATA
	} else {
		return value, fuse.OK
	}
}

func (inode *InodeCommon) listOrphanChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::listOrphanChildXAttr").Out()
	defer inode.unlinkLock.Lock().Unlock()

	inode.parseExtendedAttributes_(c)

	var nameBuffer bytes.Buffer
	for name := range inode.unlinkXAttr {
		c.vlog("Appending %s", name)
		nameBuffer.WriteString(name)
		nameBuffer.WriteByte(0)
	}

	// don't append our self-defined extended attribute XAttrTypeKey to hide it

	c.vlog("Returning %d bytes", nameBuffer.Len())

	return nameBuffer.Bytes(), fuse.OK
}

func (inode *InodeCommon) setOrphanChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.funcIn("InodeCommon::setOrphanChildXAttr").Out()
	defer inode.unlinkLock.Lock().Unlock()

	inode.parseExtendedAttributes_(c)

	// copy the data as the memory backing it will
	// be reused once this function returns
	inode.unlinkXAttr[attr] = append([]byte(nil), data...)
	inode.unlinkRecord.SetContentTime(quantumfs.NewTime(time.Now()))

	return fuse.OK
}

func (inode *InodeCommon) removeOrphanChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.funcIn("InodeCommon::removeOrphanChildXAttr").Out()
	defer inode.unlinkLock.Lock().Unlock()

	inode.parseExtendedAttributes_(c)

	if _, ok := inode.unlinkXAttr[attr]; !ok {
		return fuse.ENODATA
	}

	delete(inode.unlinkXAttr, attr)
	inode.unlinkRecord.SetContentTime(quantumfs.NewTime(time.Now()))

	return fuse.OK
}

func (inode *InodeCommon) getOrphanChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	defer c.funcIn("InodeCommon::getOrphanChildRecordCopy").Out()
	defer inode.unlinkLock.Lock().Unlock()

	if inode.unlinkRecord == nil {
		panic("getChildRecord on self file before unlinking")
	}

	return inode.unlinkRecord.AsImmutable(), nil
}

func (inode *InodeCommon) getOrphanChildAttr(c *ctx, inodeNum InodeId,
	out *fuse.Attr, owner fuse.Owner) {

	defer c.funcIn("InodeCommon::getOrphanChildAttr").Out()
	defer inode.unlinkLock.Lock().Unlock()

	utils.Assert(inode.unlinkRecord != nil,
		"getOrphanChildAttr on self before unlinking")

	fillAttrWithDirectoryRecord(c, out, inodeNum, owner, inode.unlinkRecord)
}

func (inode *InodeCommon) setChildRecord(c *ctx, record quantumfs.DirectoryRecord) {
	defer c.funcIn("InodeCommon::setChildRecord").Out()

	defer inode.unlinkLock.Lock().Unlock()

	if inode.unlinkRecord != nil {
		panic("setChildRecord on self file after unlinking")
	}

	inode.unlinkRecord = record
}

func (inode *InodeCommon) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn, out *fuse.AttrOut,
	updateMtime bool) fuse.Status {

	defer c.funcIn("InodeCommon::setChildAttr").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid setChildAttr on InodeCommon")
		return fuse.EIO
	}
	return inode.setOrphanChildAttr(c, inodeNum, newType, attr, out, updateMtime)
}

func (inode *InodeCommon) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("InodeCommon::getChildXAttrSize").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid getChildXAttrSize on InodeCommon")
		return 0, fuse.EIO
	}
	return inode.getOrphanChildXAttrSize(c, inodeNum, attr)
}

func (inode *InodeCommon) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::getChildXAttrData").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid getChildXAttrData on InodeCommon")
		return nil, fuse.EIO
	}
	return inode.getOrphanChildXAttrData(c, inodeNum, attr)
}

func (inode *InodeCommon) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::listChildXAttr").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid listChildXAttr on InodeCommon")
		return nil, fuse.EIO
	}
	return inode.listOrphanChildXAttr(c, inodeNum)
}

func (inode *InodeCommon) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.funcIn("InodeCommon::setChildXAttr").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid setChildXAttr on InodeCommon")
		return fuse.EIO
	}
	return inode.setOrphanChildXAttr(c, inodeNum, attr, data)
}

func (inode *InodeCommon) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.funcIn("InodeCommon::removeChildXAttr").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Invalid removeChildXAttr on InodeCommon")
		return fuse.EIO
	}
	return inode.removeOrphanChildXAttr(c, inodeNum, attr)
}

func (inode *InodeCommon) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	defer c.funcIn("InodeCommon::getChildRecordCopy").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		c.elog("Unsupported record fetch on file")
		return nil, errors.New("Unsupported record fetch")
	}
	return inode.getOrphanChildRecordCopy(c, inodeNum)
}

func (inode *InodeCommon) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	defer c.funcIn("InodeCommon::getChildAttr").Out()

	if !inode.isOrphaned() || inode.id != inodeNum {
		panic("Unsupported record fetch on file")

	}
	inode.getOrphanChildAttr(c, inodeNum, out, owner)
}
