// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"

// Should implement DirectoryRecordIf
type Hardlink struct {
	linkId		uint64
	wsr		*WorkspaceRoot
}

func (link *Hardlink) get() *quantumfs.DirectoryRecord {
	link_ := link.wsr.getHardlink(link.linkId)
	return &link_
}

func (link *Hardlink) set(fnSetter func (dir *quantumfs.DirectoryRecord)) {
	link.wsr.setHardlink(link.linkId, fnSetter)
}

func (link *Hardlink) Filename() string {
	return link.get().Filename()
}

func (link *Hardlink) SetFilename(v string) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetFilename(v)
	})
}

func (link *Hardlink) ID() quantumfs.ObjectKey {
	return link.get().ID()
}

func (link *Hardlink) SetID(v quantumfs.ObjectKey) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetID(v)
	})
}

func (link *Hardlink) Type() quantumfs.ObjectType {
	return link.get().Type()
}

func (link *Hardlink) SetType(v quantumfs.ObjectType) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetType(v)
	})
}

func (link *Hardlink) Permissions() uint32 {
	return link.get().Permissions()
}

func (link *Hardlink) SetPermissions(v uint32) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetPermissions(v)
	})
}

func (link *Hardlink) Owner() quantumfs.UID {
	return link.get().Owner()
}

func (link *Hardlink) SetOwner(v quantumfs.UID) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetOwner(v)
	})
}

func (link *Hardlink) Group() quantumfs.GID {
	return link.get().Group()
}

func (link *Hardlink) SetGroup(v quantumfs.GID) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetGroup(v)
	})
}

func (link *Hardlink) Size() uint64 {
	return link.get().Size()
}

func (link *Hardlink) SetSize(v uint64) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetSize(v)
	})
}

func (link *Hardlink) ExtendedAttributes() quantumfs.ObjectKey {
	return link.get().ExtendedAttributes()
}

func (link *Hardlink) SetExtendedAttributes(v quantumfs.ObjectKey) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetExtendedAttributes(v)
	})
}

func (link *Hardlink) ContentTime() quantumfs.Time {
	return link.get().ContentTime()
}

func (link *Hardlink) SetContentTime(v quantumfs.Time) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetContentTime(v)
	})
}

func (link *Hardlink) ModificationTime() quantumfs.Time {
	return link.get().ModificationTime()
}

func (link *Hardlink) SetModificationTime(v quantumfs.Time) {
	link.set(func (dir *quantumfs.DirectoryRecord) {
		dir.SetModificationTime(v)
	})
}

func (link *Hardlink) Record() quantumfs.DirectoryRecord {
	return *(link.get())
}
