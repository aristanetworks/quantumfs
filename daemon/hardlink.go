// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// Should implement quantumfs.DirectoryRecord
type Hardlink struct {
	name   string
	fileId quantumfs.FileId

	// hidden field only used for merging. Stored in the ContentTime field.
	creationTime quantumfs.Time

	hardlinkTable HardlinkTable

	publishRecord quantumfs.DirectoryRecord
}

func newHardlink(name string, fileId quantumfs.FileId, creationTime quantumfs.Time,
	hardlinkTable HardlinkTable) *Hardlink {

	utils.Assert(fileId != quantumfs.InvalidFileId,
		"invalid fileId for %s", name)
	var newLink Hardlink
	newLink.name = name
	newLink.hardlinkTable = hardlinkTable
	newLink.fileId = fileId
	newLink.creationTime = creationTime

	publishRecord := quantumfs.NewDirectoryRecord()
	publishRecord.SetType(quantumfs.ObjectTypeHardlink)
	publishRecord.SetID(newLink.ID())
	publishRecord.SetFileId(newLink.fileId)
	publishRecord.SetExtendedAttributes(quantumfs.EmptyBlockKey)

	newLink.publishRecord = publishRecord

	return &newLink
}

func (link *Hardlink) get() quantumfs.ImmutableDirectoryRecord {
	valid, link_ := link.hardlinkTable.getHardlink(link.fileId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.fileId))
	}

	return link_
}

func (link *Hardlink) set(fnSetter func(dir quantumfs.DirectoryRecord)) {
	link.hardlinkTable.setHardlink(link.fileId, fnSetter)
}

func (link *Hardlink) Filename() string {
	return link.name
}

func (link *Hardlink) SetFilename(v string) {
	link.name = v
}

func (link *Hardlink) ID() quantumfs.ObjectKey {
	var empty [quantumfs.ObjectKeyLength - 1]byte
	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, empty)
}

func (link *Hardlink) SetID(v quantumfs.ObjectKey) {
	utils.Assert(v.Type() == quantumfs.KeyTypeEmbedded,
		"invalid key type %d", v.Type())
}

func (link *Hardlink) Type() quantumfs.ObjectType {
	// Don't return the actual file type, just the hardlink
	return quantumfs.ObjectTypeHardlink
}

func (link *Hardlink) SetType(v quantumfs.ObjectType) {
	if v == quantumfs.ObjectTypeHardlink {
		panic("SetType called making hardlink")
	}

	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetType(v)
	})
}

func (link *Hardlink) Permissions() uint32 {
	return link.get().Permissions()
}

func (link *Hardlink) SetPermissions(v uint32) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetPermissions(v)
	})
}

func (link *Hardlink) Owner() quantumfs.UID {
	return link.get().Owner()
}

func (link *Hardlink) SetOwner(v quantumfs.UID) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetOwner(v)
	})
}

func (link *Hardlink) Group() quantumfs.GID {
	return link.get().Group()
}

func (link *Hardlink) SetGroup(v quantumfs.GID) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetGroup(v)
	})
}

func (link *Hardlink) Size() uint64 {
	return link.get().Size()
}

func (link *Hardlink) SetSize(v uint64) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetSize(v)
	})
}

func (link *Hardlink) ExtendedAttributes() quantumfs.ObjectKey {
	return link.get().ExtendedAttributes()
}

func (link *Hardlink) SetExtendedAttributes(v quantumfs.ObjectKey) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetExtendedAttributes(v)
	})
}

func (link *Hardlink) ContentTime() quantumfs.Time {
	return link.get().ContentTime()
}

func (link *Hardlink) SetContentTime(v quantumfs.Time) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetContentTime(v)
	})
}

func (link *Hardlink) ModificationTime() quantumfs.Time {
	return link.get().ModificationTime()
}

func (link *Hardlink) SetModificationTime(v quantumfs.Time) {
	link.set(func(dir quantumfs.DirectoryRecord) {
		dir.SetModificationTime(v)
	})
}

func (link *Hardlink) FileId() quantumfs.FileId {
	return link.fileId
}

func (link *Hardlink) SetFileId(fileId quantumfs.FileId) {
	utils.Assert(fileId == link.fileId,
		"attempt to change fileId %d -> %d", fileId, link.fileId)
}

func (link *Hardlink) Publishable() quantumfs.PublishableRecord {

	// The immutable parts of the publishRecord are always correct,
	// just update the file name and creationTime as they might have
	// changed.
	link.publishRecord.SetFilename(link.name)
	link.publishRecord.SetContentTime(link.creationTime)

	return quantumfs.AsPublishableRecord(link.publishRecord)
}

func (link *Hardlink) Nlinks() uint32 {
	return link.hardlinkTable.nlinks(link.fileId)
}

func (link *Hardlink) EncodeExtendedKey() []byte {
	valid, realRecord := link.hardlinkTable.getHardlink(link.fileId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.fileId))
	}

	return quantumfs.EncodeExtendedKey(realRecord.ID(), realRecord.Type(),
		realRecord.Size())
}

func (l *Hardlink) AsImmutableDirectoryRecord() quantumfs.ImmutableDirectoryRecord {
	// Sorry, this seems to be the only way to get the signature under 85
	// characters per line and appease gofmt.
	link := l
	valid, realRecord := link.hardlinkTable.getHardlink(link.fileId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.fileId))
	}

	return quantumfs.NewImmutableRecord(
		link.Filename(),
		realRecord.ID(),
		realRecord.Type(),
		link.Permissions(),
		link.Owner(),
		link.Group(),
		realRecord.Size(),
		link.ExtendedAttributes(),
		link.ContentTime(),
		link.ModificationTime(),
		link.Nlinks(),
		link.FileId(),
	)
}

func (link *Hardlink) Clone() quantumfs.DirectoryRecord {
	return newHardlink(link.name, link.fileId, link.creationTime,
		link.hardlinkTable)
}
