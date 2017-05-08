// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	wsr *WorkspaceRoot

	records recordsOnDemand
}

func newChildMap(c *ctx, key quantumfs.ObjectKey, wsr_ *WorkspaceRoot) (*ChildMap,
	[]InodeId) {

	rtn := ChildMap{
		wsr:     wsr_,
		records: newRecordsOnDemand(key, wsr_),
	}

	// allocate inode ids
	uninstantiated := make([]InodeId, 0)
	rtn.records.iterateOverRecords(c,
		func(record quantumfs.DirectoryRecord) {

			inodeId := rtn.loadInodeId(c, record,
				quantumfs.InodeIdInvalid)
			rtn.records.mapInodeId(record.Filename(), inodeId)
			uninstantiated = append(uninstantiated, inodeId)
		})

	return &rtn, uninstantiated
}

// Whenever a record passes through this class, we must ensure it's converted if
// necessary
func convertRecord(wsr *WorkspaceRoot,
	entry quantumfs.DirectoryRecord) quantumfs.DirectoryRecord {

	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		entry = newHardlink(entry.Filename(), linkId, wsr)
	}

	return entry
}

func (cmap *ChildMap) recordByName(c *ctx, name string) quantumfs.DirectoryRecord {
	defer c.FuncIn("ChildMap::recordByName", "%s", name).Out()

	// Do everything we can to optimize this function and allow fast escape
	if _, exists := cmap.records.inodeId(name); !exists {
		return nil
	}

	record, exists := cmap.records.getCached(name)
	if exists {
		return record
	}

	return cmap.records.fetchFromBase(c, name)
}

// Returns the inodeId used for the child
func (cmap *ChildMap) loadInodeId(c *ctx, entry quantumfs.DirectoryRecord,
	inodeId InodeId) InodeId {

	defer c.FuncIn("ChildMap::loadInodeId", "inode %d", inodeId).Out()

	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		establishedInodeId := cmap.wsr.getHardlinkInodeId(c, linkId)

		// If you try to load a hardlink and provide a real inodeId, it
		// should match the actual inodeId for the hardlink or else
		// something is really wrong in the system
		if inodeId != quantumfs.InodeIdInvalid &&
			inodeId != establishedInodeId {

			c.elog("Attempt to set hardlink with mismatched inodeId, "+
				"%d vs %d", inodeId, establishedInodeId)
		}
		inodeId = establishedInodeId
	} else if inodeId == quantumfs.InodeIdInvalid {
		inodeId = c.qfs.newInodeId()
	}

	return inodeId
}

// Returns the inodeId used for the child
func (cmap *ChildMap) loadChild(c *ctx, entry quantumfs.DirectoryRecord,
	inodeId InodeId) InodeId {

	defer c.FuncIn("ChildMap::loadChild", "%s %s", entry.Filename(),
		entry.ID().Text()).Out()

	entry = convertRecord(cmap.wsr, entry)
	if entry == nil {
		panic(fmt.Sprintf("Nil DirectoryEntryIf set attempt: %d", inodeId))
	}

	inodeId = cmap.loadInodeId(c, entry, inodeId)

	// child is not dirty by default
	cmap.records.setRecord(entry, inodeId)

	return inodeId
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.records.nameToInode))
}

func (cmap *ChildMap) deleteChild(c *ctx,
	name string) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildMap::deleteChild", "name %s", name).Out()

	inodeId, exists := cmap.records.inodeId(name)
	if !exists {
		c.vlog("name does not exist")
		return nil
	}

	record := cmap.recordByName(c, name)
	if record == nil {
		c.vlog("record does not exist")
		return nil
	}

	// This may be a hardlink that is due to be converted.
	if hardlink, isHardlink := record.(*Hardlink); isHardlink {
		var newRecord quantumfs.DirectoryRecord
		newRecord, removeId := cmap.wsr.removeHardlink(c,
			hardlink.linkId)

		// Wsr says we're about to orphan the last hardlink copy
		if newRecord != nil || removeId != quantumfs.InodeIdInvalid {
			newRecord.SetFilename(hardlink.Filename())
			record = newRecord
			cmap.loadChild(c, newRecord, removeId)
		}
	}
	result := cmap.recordByName(c, name)
	cmap.records.delRecord(name, inodeId)

	if link, isHardlink := record.(*Hardlink); isHardlink {
		if cmap.wsr.hardlinkDec(link.linkId) {
			// If the refcount was greater than one we shouldn't
			// reparent.
			c.vlog("Hardlink referenced elsewhere")
			return nil
		}
	}
	return result
}

func (cmap *ChildMap) renameChild(c *ctx, oldName string,
	newName string) (oldInodeRemoved InodeId) {

	defer c.FuncIn("ChildMap::renameChild", "oldName %s newName %s", oldName,
		newName).Out()

	if oldName == newName {
		c.vlog("Names are identical")
		return quantumfs.InodeIdInvalid
	}

	inodeId, exists := cmap.records.inodeId(oldName)
	if !exists {
		c.vlog("oldName doesn't exist")
		return quantumfs.InodeIdInvalid
	}

	record := cmap.recordByName(c, oldName)
	if record == nil {
		c.vlog("oldName record doesn't exist")
		panic("inode set without record")
	}

	// record whether we need to cleanup a file we're overwriting
	cleanupInodeId, needCleanup := cmap.records.inodeId(newName)
	if needCleanup {
		// we have to cleanup before we move, to allow the case where we
		// rename a hardlink to an existing one with the same inode
		cmap.records.delRecord(newName, cleanupInodeId)
	}

	cmap.records.delRecord(oldName, inodeId)

	record.SetFilename(newName)
	cmap.records.setRecord(record, inodeId)

	if needCleanup {
		c.vlog("cleanupInodeId %d", cleanupInodeId)
		return cleanupInodeId
	}

	return quantumfs.InodeIdInvalid
}

func (cmap *ChildMap) inodeNum(name string) InodeId {
	if inodeId, exists := cmap.records.inodeId(name); exists {
		return inodeId
	}

	return quantumfs.InodeIdInvalid
}

func (cmap *ChildMap) directInodes(c *ctx) []InodeId {
	rtn := make([]InodeId, 0)

	for k, _ := range cmap.records.inodeToName {
		if isHardlink, _ := cmap.wsr.checkHardlink(k); !isHardlink {
			rtn = append(rtn, k)
		}
	}

	return rtn
}

func (cmap *ChildMap) recordCopies(c *ctx) []quantumfs.DirectoryRecord {
	rtn := make([]quantumfs.DirectoryRecord, 0)

	cmap.records.iterateOverRecords(c,
		func(record quantumfs.DirectoryRecord) {

			rtn = append(rtn, record)
		})

	return rtn
}

func (cmap *ChildMap) recordCopy(c *ctx,
	inodeId InodeId) quantumfs.DirectoryRecord {

	// check if there's an entry first
	recordName, exists := cmap.records.firstName(inodeId)
	if !exists {
		return nil
	}

	// Check if the dirty cache already has an entry
	record, exists := cmap.records.getCached(recordName)
	if exists {
		return record
	}

	return cmap.records.fetchFromBase(c, recordName)
}

func (cmap *ChildMap) setKey(inodeNum InodeId, key quantumfs.ObjectKey) fuse.Status {
	return cmap.records.setKey(inodeNum, key)
}

func (cmap *ChildMap) makeHardlink(c *ctx,
	childId InodeId) (copy quantumfs.DirectoryRecord, err fuse.Status) {

	defer c.FuncIn("ChildMap::makeHardlink", "inode %d", childId).Out()

	child := cmap.recordCopy(c, childId)
	if child == nil {
		c.elog("No child record for inode id %d in childmap", childId)
		return nil, fuse.ENOENT
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*Hardlink); isLink {
		c.vlog("Already a hardlink")

		recordCopy := *link

		// Ensure we update the ref count for this hardlink
		cmap.wsr.hardlinkInc(link.linkId)

		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if child.Type() != quantumfs.ObjectTypeSmallFile &&
		child.Type() != quantumfs.ObjectTypeMediumFile &&
		child.Type() != quantumfs.ObjectTypeLargeFile &&
		child.Type() != quantumfs.ObjectTypeVeryLargeFile &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.dlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, fuse.EINVAL
	}

	// It needs to become a hardlink now. Hand it off to wsr
	c.vlog("Converting into a hardlink")
	newLink := cmap.wsr.newHardlink(c, childId, child)

	cmap.records.setRecord(newLink, childId)
	linkCopy := *newLink
	return &linkCopy, fuse.OK
}

func (cmap *ChildMap) publish(c *ctx) quantumfs.ObjectKey {
	return cmap.records.publish(c)
}

type recordsOnDemand struct {
	wsr  *WorkspaceRoot
	base quantumfs.ObjectKey

	// can be many to one
	nameToInode map[string]InodeId
	inodeToName map[InodeId][]string

	nameToEntryIdx map[string]uint32

	cache map[string]quantumfs.DirectoryRecord
	// One of the most common and expensive operations is updating the ID.
	// Separate this into its own cache to optimize syncChild
	cacheKey map[InodeId]quantumfs.ObjectKey
}

func newRecordsOnDemand(key quantumfs.ObjectKey,
	wsr_ *WorkspaceRoot) recordsOnDemand {

	return recordsOnDemand{
		wsr:            wsr_,
		base:           key,
		nameToInode:    make(map[string]InodeId),
		inodeToName:    make(map[InodeId][]string),
		nameToEntryIdx: make(map[string]uint32),
		cache:          make(map[string]quantumfs.DirectoryRecord),
		cacheKey:       make(map[InodeId]quantumfs.ObjectKey),
	}
}

func (rd *recordsOnDemand) inodeId(name string) (InodeId, bool) {
	inodeId, exists := rd.nameToInode[name]
	return inodeId, exists
}

func (rd *recordsOnDemand) firstName(inodeId InodeId) (string, bool) {
	list, exists := rd.inodeToName[inodeId]
	if !exists {
		return "", false
	}

	return list[0], true
}

func (rd *recordsOnDemand) mapInodeId(name string, inodeId InodeId) {
	rd.nameToInode[name] = inodeId

	list, exists := rd.inodeToName[inodeId]
	if exists {
		// only add if it doesn't exist already
		for _, v := range list {
			if v == name {
				// nothing more to do
				return
			}
		}
	} else {
		list = make([]string, 0)
	}

	list = append(list, name)
	rd.inodeToName[inodeId] = list
}

func (rd *recordsOnDemand) fetchFromBase(c *ctx,
	name string) quantumfs.DirectoryRecord {

	nameOffset, exists := rd.nameToEntryIdx[name]
	if !exists {
		return nil
	}
	offset := int(nameOffset)

	key := rd.base
	for {
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}

		baseLayer := buffer.AsDirectoryEntry()

		if int(baseLayer.NumEntries()) > offset {
			// the record should be in this DirectoryEntry
			entry := quantumfs.DirectoryRecord(baseLayer.Entry(offset))
			// Clone to ensure that golang frees the rest of the memory
			entry = convertRecord(rd.wsr, entry).Clone()

			if entry.Filename() != name {
				c.elog("Name map mismatch: %s vs %s", name,
					entry.Filename())
			}

			// ensure we use the newest key
			entryInodeId := rd.nameToInode[entry.Filename()]
			newerKey, useNewer := rd.cacheKey[entryInodeId]
			if useNewer {
				entry.SetID(newerKey)
			}

			return entry
		} else {
			offset -= baseLayer.NumEntries()

			// go to the next page
			if baseLayer.HasNext() {
				key = baseLayer.Next()
			} else {
				break
			}
		}
	}

	return nil
}

func (rd *recordsOnDemand) iterateOverRecords(c *ctx,
	fxn func(quantumfs.DirectoryRecord)) {

	existingEntries := make(map[string]bool, 0)
	entryIdx := uint32(0)

	key := rd.base
	for {
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic(fmt.Sprintf("No baseLayer object for %s", key.Text()))
		}

		baseLayer := buffer.AsDirectoryEntry()

		for i := 0; i < baseLayer.NumEntries(); i++ {
			entry := quantumfs.DirectoryRecord(baseLayer.Entry(i))
			// remember the index for fast access later
			rd.nameToEntryIdx[entry.Filename()] = entryIdx

			// ensure we overwrite changes from the base
			record, exists := rd.getCached(entry.Filename())
			if exists {
				// if the record is nil, that means it was deleted
				if record != nil {
					fxn(record)
				}
			} else {
				// Ensure we Clone() so the rest of memory is freed
				entry = convertRecord(rd.wsr, entry).Clone()

				// ensure we use the newest key
				entryInodeId := rd.nameToInode[entry.Filename()]
				newerKey, useNewer := rd.cacheKey[entryInodeId]
				if useNewer {
					entry.SetID(newerKey)
				}

				fxn(entry)
			}

			existingEntries[entry.Filename()] = true
			entryIdx++
		}

		if baseLayer.HasNext() {
			key = baseLayer.Next()
		} else {
			break
		}
	}

	// don't forget added entries
	for name, record := range rd.cache {
		if record == nil {
			continue
		}

		if _, exists := existingEntries[name]; !exists {
			recordInodeId := rd.nameToInode[record.Filename()]
			newerKey, useNewer := rd.cacheKey[recordInodeId]
			if useNewer {
				record.SetID(newerKey)
			}

			fxn(record)
		}
	}
}

func (rd *recordsOnDemand) setRecord(record quantumfs.DirectoryRecord,
	inodeId InodeId) {

	rd.cache[record.Filename()] = record
	rd.mapInodeId(record.Filename(), inodeId)
	delete(rd.cacheKey, inodeId)
}

func (rd *recordsOnDemand) setKey(inodeNum InodeId,
	key quantumfs.ObjectKey) fuse.Status {

	if _, exists := rd.inodeToName[inodeNum]; !exists {
		return fuse.ENOENT
	}

	rd.cacheKey[inodeNum] = key
	return fuse.OK
}

func (rd *recordsOnDemand) getCached(name string) (quantumfs.DirectoryRecord, bool) {
	record, exists := rd.cache[name]
	if !exists {
		return nil, false
	}

	if record == nil {
		return nil, true
	}

	recordInodeId := rd.nameToInode[record.Filename()]
	newerKey, exists := rd.cacheKey[recordInodeId]
	if exists {
		record.SetID(newerKey)
	}

	return record, true
}

func (rd *recordsOnDemand) delRecord(name string, inodeId InodeId) {
	delete(rd.nameToInode, name)

	list, exists := rd.inodeToName[inodeId]
	if exists {
		for i := 0; i < len(list); i++ {
			if list[i] == name {
				list = append(list[:i], list[i+1:]...)

				if len(list) > 0 {
					rd.inodeToName[inodeId] = list
				} else {
					delete(rd.inodeToName, inodeId)
				}
				break
			}
		}

	}

	rd.cache[name] = nil
	delete(rd.cacheKey, inodeId)
}

func (rd *recordsOnDemand) publish(c *ctx) quantumfs.ObjectKey {

	defer c.funcIn("recordsOnDemand::publish").Out()

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.
	newBaseLayerId := quantumfs.EmptyDirKey

	// childIdx indexes into dir.records, entryIdx indexes into the
	// metadata block
	baseLayer := quantumfs.NewDirectoryEntry()
	entryIdx := 0
	rd.iterateOverRecords(c, func(record quantumfs.DirectoryRecord) {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			c.vlog("Block full with %d entries", entryIdx)
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
			entryIdx = 0
		}

		recordCopy := record.Record()
		baseLayer.SetEntry(entryIdx, &recordCopy)

		entryIdx++
	})

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)

	// update our state
	rd.base = newBaseLayerId
	rd.cache = make(map[string]quantumfs.DirectoryRecord)
	rd.cacheKey = make(map[InodeId]quantumfs.ObjectKey)

	// re-set our map of indices into directory entries
	rd.nameToEntryIdx = make(map[string]uint32)
	rd.iterateOverRecords(c, func(record quantumfs.DirectoryRecord) {
		// don't need to do anything while we iterate
	})

	return newBaseLayerId
}

func publishDirectoryEntry(c *ctx, layer *quantumfs.DirectoryEntry,
	nextKey quantumfs.ObjectKey) quantumfs.ObjectKey {

	defer c.funcIn("publishDirectoryEntry").Out()

	layer.SetNext(nextKey)
	bytes := layer.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newKey, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new baseLayer object")
	}

	return newKey
}
