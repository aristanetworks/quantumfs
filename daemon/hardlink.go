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

	// hidden field only used for merging
	created quantumfs.Time

	wsr *WorkspaceRoot
}

func newHardlink(name string, fileId quantumfs.FileId, created quantumfs.Time,
	wsr *WorkspaceRoot) *Hardlink {

	utils.Assert(fileId != quantumfs.InvalidFileId,
		"invalid fileId for %s", name)
	var newLink Hardlink
	newLink.name = name
	newLink.wsr = wsr
	newLink.fileId = fileId
	newLink.created = created

	return &newLink
}

func (link *Hardlink) get() *quantumfs.DirectRecord {
	valid, link_ := link.wsr.getHardlink(link.fileId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.fileId))
	}

	return &link_
}

func (link *Hardlink) set(fnSetter func(dir *quantumfs.DirectRecord)) {
	link.wsr.setHardlink(link.fileId, fnSetter)
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

	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetType(v)
	})
}

func (link *Hardlink) Permissions() uint32 {
	return link.get().Permissions()
}

func (link *Hardlink) SetPermissions(v uint32) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetPermissions(v)
	})
}

func (link *Hardlink) Owner() quantumfs.UID {
	return link.get().Owner()
}

func (link *Hardlink) SetOwner(v quantumfs.UID) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetOwner(v)
	})
}

func (link *Hardlink) Group() quantumfs.GID {
	return link.get().Group()
}

func (link *Hardlink) SetGroup(v quantumfs.GID) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetGroup(v)
	})
}

func (link *Hardlink) Size() uint64 {
	return link.get().Size()
}

func (link *Hardlink) SetSize(v uint64) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetSize(v)
	})
}

func (link *Hardlink) ExtendedAttributes() quantumfs.ObjectKey {
	return link.get().ExtendedAttributes()
}

func (link *Hardlink) SetExtendedAttributes(v quantumfs.ObjectKey) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetExtendedAttributes(v)
	})
}

func (link *Hardlink) ContentTime() quantumfs.Time {
	return link.get().ContentTime()
}

func (link *Hardlink) SetContentTime(v quantumfs.Time) {
	link.set(func(dir *quantumfs.DirectRecord) {
		dir.SetContentTime(v)
	})
}

func (link *Hardlink) ModificationTime() quantumfs.Time {
	return link.get().ModificationTime()
}

func (link *Hardlink) SetModificationTime(v quantumfs.Time) {
	link.set(func(dir *quantumfs.DirectRecord) {
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

func (link *Hardlink) Record() quantumfs.DirectRecord {
	// Note: this is a DirectRecord shallow copy type
	rtn := quantumfs.NewDirectoryRecord()
	rtn.SetType(quantumfs.ObjectTypeHardlink)
	rtn.SetFilename(link.name)
	rtn.SetFileId(link.fileId)
	rtn.SetContentTime(link.created)
	rtn.SetID(link.ID())

	// we only need to return a thin record - just enough information to
	// create the hardlink. The rest is stored in workspaceroot.

	return *rtn
}

func (link *Hardlink) Nlinks() uint32 {
	return link.wsr.nlinks(link.fileId)
}

func (link *Hardlink) EncodeExtendedKey() []byte {
	valid, realRecord := link.wsr.getHardlink(link.fileId)
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
	valid, realRecord := link.wsr.getHardlink(link.fileId)
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
	return newHardlink(link.name, link.fileId, link.created, link.wsr)
}
