// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// implements quantumfs.DirectoryRecord
type HardlinkLeg struct {
	hardlinkTable HardlinkTable
	record        quantumfs.DirectoryRecord
}

func newHardlinkLeg(name string, fileId quantumfs.FileId,
	creationTime quantumfs.Time, hardlinkTable HardlinkTable) *HardlinkLeg {

	utils.Assert(fileId != quantumfs.InvalidFileId,
		"invalid fileId for %s", name)

	var newLink HardlinkLeg

	record := quantumfs.NewDirectoryRecord()
	record.SetType(quantumfs.ObjectTypeHardlink)
	record.SetID(newLink.ID())
	record.SetFileId(fileId)
	record.SetExtendedAttributes(quantumfs.EmptyBlockKey)
	record.SetFilename(name)
	record.SetContentTime(creationTime)

	newLink.record = record
	newLink.hardlinkTable = hardlinkTable

	return &newLink
}

func newHardlinkLegFromRecord(record quantumfs.ImmutableDirectoryRecord,
	hardlinkTable HardlinkTable) quantumfs.DirectoryRecord {

	_, isHardlinkLeg := record.(*HardlinkLeg)
	utils.Assert(!isHardlinkLeg, "Cannot re-wrap HardlinkLeg")

	return newHardlinkLeg(record.Filename(), record.FileId(),
		record.ContentTime(), hardlinkTable)
}

func (link *HardlinkLeg) get() quantumfs.ImmutableDirectoryRecord {
	link_ := link.hardlinkTable.recordByFileId(link.FileId())

	// This object shouldn't even exist if the hardlink's invalid
	utils.Assert(link_ != nil, "Unable to get record for existing link %d",
		link.FileId())

	return link_
}

func (link *HardlinkLeg) Filename() string {
	return link.record.Filename()
}

func (link *HardlinkLeg) SetFilename(v string) {
	link.record.SetFilename(v)

}

func (link *HardlinkLeg) ID() quantumfs.ObjectKey {
	var empty [quantumfs.ObjectKeyLength - 1]byte
	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, empty)
}

func (link *HardlinkLeg) SetID(v quantumfs.ObjectKey) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Type() quantumfs.ObjectType {
	// Don't return the actual file type, just the hardlink
	return quantumfs.ObjectTypeHardlink
}

func (link *HardlinkLeg) SetType(v quantumfs.ObjectType) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Permissions() uint32 {
	return link.get().Permissions()
}

func (link *HardlinkLeg) SetPermissions(v uint32) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Owner() quantumfs.UID {
	return link.get().Owner()
}

func (link *HardlinkLeg) SetOwner(v quantumfs.UID) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Group() quantumfs.GID {
	return link.get().Group()
}

func (link *HardlinkLeg) SetGroup(v quantumfs.GID) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Size() uint64 {
	return link.get().Size()
}

func (link *HardlinkLeg) SetSize(v uint64) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) ExtendedAttributes() quantumfs.ObjectKey {
	return link.get().ExtendedAttributes()
}

func (link *HardlinkLeg) SetExtendedAttributes(v quantumfs.ObjectKey) {
	panic("Cannot set on HardlinkLeg")
}

// The creationTime of a hardlink is used for merging and persists
// in the contentTime field of the hardlink leg
func (link *HardlinkLeg) creationTime() quantumfs.Time {
	return link.record.ContentTime()
}

func (link *HardlinkLeg) setCreationTime(v quantumfs.Time) {
	link.record.SetContentTime(v)
}

func (link *HardlinkLeg) ContentTime() quantumfs.Time {
	return link.get().ContentTime()
}

func (link *HardlinkLeg) SetContentTime(v quantumfs.Time) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) ModificationTime() quantumfs.Time {
	return link.get().ModificationTime()
}

func (link *HardlinkLeg) SetModificationTime(v quantumfs.Time) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) FileId() quantumfs.FileId {
	return link.record.FileId()
}

func (link *HardlinkLeg) SetFileId(fileId quantumfs.FileId) {
	panic("Cannot set on HardlinkLeg")
}

func (link *HardlinkLeg) Publishable() quantumfs.PublishableRecord {
	return quantumfs.AsPublishableRecord(link.record)
}

func (link *HardlinkLeg) Nlinks() uint32 {
	return link.hardlinkTable.nlinks(link.FileId())
}

func (link *HardlinkLeg) EncodeExtendedKey() []byte {
	realRecord := link.hardlinkTable.recordByFileId(link.FileId())

	// This object shouldn't even exist if the hardlink's invalid
	utils.Assert(realRecord != nil, "Unable to get record for existing link %d",
		link.FileId())

	return quantumfs.EncodeExtendedKey(realRecord.ID(), realRecord.Type(),
		realRecord.Size())
}

func (l *HardlinkLeg) AsImmutable() quantumfs.ImmutableDirectoryRecord {
	// Sorry, this seems to be the only way to get the signature under 85
	// characters per line and appease gofmt.
	link := l

	realRecord := link.hardlinkTable.recordByFileId(link.FileId())

	// This object shouldn't even exist if the hardlink's invalid
	utils.Assert(realRecord != nil, "Unable to get record for existing link %d",
		link.FileId())

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

func (link *HardlinkLeg) Clone() quantumfs.DirectoryRecord {
	return newHardlinkLeg(link.Filename(), link.FileId(), link.creationTime(),
		link.hardlinkTable)
}

func underlyingType(r quantumfs.ImmutableDirectoryRecord) quantumfs.ObjectType {
	record := r

	if hll, ok := record.(*HardlinkLeg); ok {
		return hll.get().Type()
	}
	return record.Type()
}

func underlyingID(r quantumfs.ImmutableDirectoryRecord) quantumfs.ObjectKey {
	record := r

	if hll, ok := record.(*HardlinkLeg); ok {
		return hll.get().ID()
	}
	return record.ID()
}
