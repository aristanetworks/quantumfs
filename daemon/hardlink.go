// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"encoding/binary"
	"fmt"

	"github.com/aristanetworks/quantumfs"
)

type HardlinkId uint64

func (v HardlinkId) Primitive() interface{} {
	return uint64(v)
}

// Should implement quantumfs.DirectoryRecord
type Hardlink struct {
	name   string
	linkId HardlinkId
	wsr    *WorkspaceRoot
}

func decodeHardlinkKey(key quantumfs.ObjectKey) (hardlinkId HardlinkId) {
	if key.Type() != quantumfs.KeyTypeEmbedded {
		panic("Non-embedded key attempted decode as Hardlink Id")
	}

	hash := key.Hash()
	return HardlinkId(binary.LittleEndian.Uint64(hash[0:8]))
}

func encodeHardlinkId(hardlinkId HardlinkId) quantumfs.ObjectKey {
	var hash [quantumfs.ObjectKeyLength - 1]byte

	binary.LittleEndian.PutUint64(hash[0:8], uint64(hardlinkId))
	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, hash)
}

func newHardlink(name string, linkId HardlinkId, wsr *WorkspaceRoot) *Hardlink {
	var newLink Hardlink
	newLink.name = name
	newLink.wsr = wsr
	newLink.linkId = linkId

	return &newLink
}

func (link *Hardlink) get() *quantumfs.DirectRecord {
	valid, link_ := link.wsr.getHardlink(link.linkId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.linkId))
	}

	return &link_
}

func (link *Hardlink) set(fnSetter func(dir *quantumfs.DirectRecord)) {
	link.wsr.setHardlink(link.linkId, fnSetter)
}

func (link *Hardlink) Filename() string {
	return link.name
}

func (link *Hardlink) SetFilename(v string) {
	link.name = v
}

func (link *Hardlink) ID() quantumfs.ObjectKey {
	return encodeHardlinkId(link.linkId)
}

func (link *Hardlink) SetID(v quantumfs.ObjectKey) {
	decodedId := decodeHardlinkKey(v)

	if decodedId != link.linkId {
		panic(fmt.Sprintf("Change of ID attempted on Hardlink to %d",
			decodedId))
	}
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

func (link *Hardlink) Record() quantumfs.DirectRecord {
	// Note: this is a DirectRecord shallow copy type
	rtn := quantumfs.NewDirectoryRecord()
	rtn.SetType(quantumfs.ObjectTypeHardlink)
	rtn.SetID(encodeHardlinkId(link.linkId))
	rtn.SetFilename(link.name)

	// we only need to return a thin record - just enough information to
	// create the hardlink. The rest is stored in workspaceroot.

	return *rtn
}

func (link *Hardlink) Nlinks() uint32 {
	return link.wsr.nlinks(link.linkId)
}

func (link *Hardlink) EncodeExtendedKey() []byte {
	valid, realRecord := link.wsr.getHardlink(link.linkId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.linkId))
	}

	return quantumfs.EncodeExtendedKey(realRecord.ID(), realRecord.Type(),
		realRecord.Size())
}

func (l *Hardlink) AsImmutableDirectoryRecord() quantumfs.ImmutableDirectoryRecord {
	// Sorry, this seems to be the only way to get the signature under 85
	// characters per line and appease gofmt.
	link := l
	valid, realRecord := link.wsr.getHardlink(link.linkId)
	if !valid {
		// This object shouldn't even exist if the hardlink's invalid
		panic(fmt.Sprintf("Unable to get record for existing link %d",
			link.linkId))
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
	)
}

func (link *Hardlink) Clone() quantumfs.DirectoryRecord {
	return newHardlink(link.name, link.linkId, link.wsr)
}
