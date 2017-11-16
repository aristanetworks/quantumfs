// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"errors"
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// hardlinkTracker used to track and compute the final hardlink versions and ref
// counts. Assumes local will be taken and then compensates when remote is chosen.
// This allows merge to skip traversing local subtrees as an optimization.
// Note: We assume that FileId is universally unique and will never collide
type hardlinkTracker struct {
	// contains all local and remote records, with their contents merged
	allRecords map[quantumfs.FileId]*quantumfs.DirectRecord

	merged map[quantumfs.FileId]linkEntry
}

func newHardlinkTracker(c *ctx, base map[quantumfs.FileId]linkEntry,
	remote map[quantumfs.FileId]linkEntry,
	local map[quantumfs.FileId]linkEntry) *hardlinkTracker {

	rtn := hardlinkTracker{
		allRecords: make(map[quantumfs.FileId]*quantumfs.DirectRecord),
		merged:     make(map[quantumfs.FileId]linkEntry),
	}

	// Merge all records together and do intra-file merges
	for k, remoteEntry := range remote {
		rtn.allRecords[k] = remoteEntry.record
	}

	// make sure merged has the newest available record versions based off local
	for k, localEntry := range local {
		if remoteEntry, exists := remote[k]; exists {
			var baseRecord quantumfs.DirectoryRecord
			baseEntry, baseExists := base[k]
			if baseExists {
				baseRecord = baseEntry.record
			}

			mergedRecord, err := mergeFile(c, baseRecord,
				remoteEntry.record, localEntry.record)
			if err != nil {
				panic(err)
			}

			rtn.allRecords[k] = mergedRecord.(*quantumfs.DirectRecord)
		} else {
			rtn.allRecords[k] = localEntry.record
		}

		localEntry.record = rtn.allRecords[k]
		rtn.merged[k] = localEntry
	}

	return &rtn
}

func traverseSubtree(c *ctx, dirKey quantumfs.ObjectKey,
	fn func(quantumfs.DirectoryRecord)) error {

	records, err := loadRecords(c, dirKey)
	if err != nil {
		return err
	}

	for _, v := range records {
		if v.Type() == quantumfs.ObjectTypeDirectory {
			err = traverseSubtree(c, v.ID(), fn)
			if err != nil {
				return err
			}
		}

		fn(v)
	}

	return nil
}

// Compares the local record against merge product and tracks any changes
func (ht *hardlinkTracker) checkLinkChanged(c *ctx, local quantumfs.DirectoryRecord,
	final quantumfs.DirectoryRecord) {

	if final != nil && final.Type() == quantumfs.ObjectTypeHardlink {
		// This is now a new hardlink instance in the system
		ht.increment(final.FileId())
	}

	if local != nil && local.Type() == quantumfs.ObjectTypeHardlink {
		ht.decrement(local.FileId())
	}
}

func (ht *hardlinkTracker) increment(id quantumfs.FileId) {
	link := ht.newestEntry(id)
	link.nlink++

	ht.merged[id] = link
}

func (ht *hardlinkTracker) decrement(id quantumfs.FileId) {
	link := ht.newestEntry(id)

	if link.nlink <= 1 {
		delete(ht.merged, id)
		return
	}

	link.nlink--
	ht.merged[id] = link
}

// Returns the newest linkEntry version available, while preserving nlink from merged
func (ht *hardlinkTracker) newestEntry(id quantumfs.FileId) linkEntry {
	link, _ := ht.merged[id]

	// Use the latest record, but preserve the nlink count from merged
	record, exists := ht.allRecords[id]

	utils.Assert(exists, "Unable to find entry for fileId %d", id)

	link.record = record

	return link
}

func loadWorkspaceRoot(c *ctx,
	key quantumfs.ObjectKey) (hardlinks map[quantumfs.FileId]linkEntry,
	directory quantumfs.ObjectKey, err error) {

	buffer := c.dataStore.Get(&c.Ctx, key)
	if buffer == nil {
		return nil, key,
			fmt.Errorf("Unable to Get block for key: %s", key.String())
	}
	workspaceRoot := buffer.AsWorkspaceRoot()

	links := loadHardlinks(c, workspaceRoot.HardlinkEntry())

	return links, workspaceRoot.BaseLayer(), nil
}

func mergeWorkspaceRoot(c *ctx, base quantumfs.ObjectKey, remote quantumfs.ObjectKey,
	local quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.funcIn("mergeWorkspaceRoot").Out()

	baseHardlinks, baseDirectory, err := loadWorkspaceRoot(c, base)
	if err != nil {
		return local, err
	}
	remoteHardlinks, remoteDirectory, err := loadWorkspaceRoot(c, remote)
	if err != nil {
		return local, err
	}
	localHardlinks, localDirectory, err := loadWorkspaceRoot(c,
		local)
	if err != nil {
		return local, err
	}

	tracker := newHardlinkTracker(c, baseHardlinks, remoteHardlinks,
		localHardlinks)

	localDirectory, err = mergeDirectory(c, baseDirectory,
		remoteDirectory, localDirectory, true, tracker)
	if err != nil {
		return local, err
	}

	return publishWorkspaceRoot(c, localDirectory, tracker.merged), nil
}

func loadRecords(c *ctx,
	key quantumfs.ObjectKey) (map[string]quantumfs.DirectoryRecord, error) {

	rtn := make(map[string]quantumfs.DirectoryRecord)

	for {
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			return nil, fmt.Errorf("No object for key %s", key.String())
		}

		baseLayer := buffer.AsDirectoryEntry()

		for i := 0; i < baseLayer.NumEntries(); i++ {
			entry := baseLayer.Entry(i)
			rtn[entry.Filename()] = entry
		}

		if baseLayer.HasNext() {
			key = baseLayer.Next()
		} else {
			return rtn, nil
		}
	}
}

// sometimes, in theory, two workspaces could simultaneously create directories or
// records with the same name. We handle these cases like mostly normal conflicts.
func mergeDirectory(c *ctx, base quantumfs.ObjectKey,
	remote quantumfs.ObjectKey, local quantumfs.ObjectKey,
	baseExists bool, ht *hardlinkTracker) (quantumfs.ObjectKey, error) {

	defer c.funcIn("mergeDirectory").Out()

	var err error
	baseRecords := make(map[string]quantumfs.DirectoryRecord)
	if baseExists {
		baseRecords, err = loadRecords(c, base)
		if err != nil {
			return local, err
		}
	}
	remoteRecords, err := loadRecords(c, remote)
	if err != nil {
		return local, err
	}
	localRecords, err := loadRecords(c, local)
	if err != nil {
		return local, err
	}

	// make a copy to preserve localRecords
	mergedRecords := make(map[string]quantumfs.DirectoryRecord)
	for k, v := range localRecords {
		mergedRecords[k] = v
	}

	for k, v := range remoteRecords {
		baseChild, inBase := baseRecords[k]
		localChild, inLocal := localRecords[k]

		if inLocal {
			// We have at least a local and remote, must merge
			mergedRecords[k], err = mergeRecord(c, baseChild, v,
				localChild, ht)
			if err != nil {
				return local, err
			}
		} else if !inBase {
			// just take remote since it's known newer than base, but
			// only if local didn't delete it from base
			mergedRecords[k] = v

			// Add new links
			if v.Type() == quantumfs.ObjectTypeDirectory {
				err = traverseSubtree(c, v.ID(),
					func(v quantumfs.DirectoryRecord) {

						ht.checkLinkChanged(c, nil, v)
					})
				if err != nil {
					return local, err
				}
			}
		}

		// check for hardlink addition or update
		mergedRecord, _ := mergedRecords[k]
		ht.checkLinkChanged(c, localChild, mergedRecord)
	}

	if baseExists {
		for k, _ := range baseRecords {
			_, inRemote := remoteRecords[k]
			localRecord, inLocal := localRecords[k]

			// Delete iff the file was deleted in remote only,
			// (otherwise local, our reference, already deleted it and
			// we don't want to doulbly delete)
			if !inRemote && inLocal {
				c.vlog("Remote deleted %s", k)
				delete(mergedRecords, k)

				// check for hardlink deletion
				ht.checkLinkChanged(c, localRecord, nil)
			}
		}
	}

	// turn mergedRecords into a publishable format
	localRecordsList := make([]quantumfs.DirectoryRecord, 0, len(mergedRecords))
	for _, v := range mergedRecords {
		localRecordsList = append(localRecordsList, v)
	}

	// publish localRecordsList
	newBaseLayerId := quantumfs.EmptyDirKey

	entryCapacity := len(localRecordsList)
	entryCapacity, baseLayer := quantumfs.NewDirectoryEntry(entryCapacity)
	entryIdx := 0
	for _, record := range localRecordsList {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			entryCapacity, baseLayer = quantumfs.NewDirectoryEntry(
				entryCapacity)
			entryIdx = 0
		}

		recordCopy := record.Record()
		baseLayer.SetEntry(entryIdx, &recordCopy)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	return publishDirectoryEntry(c, baseLayer, newBaseLayerId), nil
}

var emptyAttrs *quantumfs.ExtendedAttributes

func init() {
	emptyAttrs = quantumfs.NewExtendedAttributes()
}

func mergeExtendedAttrs(c *ctx, base quantumfs.ObjectKey,
	newer quantumfs.ObjectKey, older quantumfs.ObjectKey) (quantumfs.ObjectKey,
	error) {

	baseAttrs, err := getRecordExtendedAttributes(c, base)
	if err == fuse.ENOENT || base.IsEqualTo(quantumfs.ZeroKey) {
		baseAttrs = emptyAttrs
	} else if err != fuse.OK {
		return quantumfs.EmptyBlockKey, errors.New("Merge ExtAttr base: " +
			err.String())
	}

	newerAttrs, err := getRecordExtendedAttributes(c, newer)
	if err == fuse.ENOENT || newer.IsEqualTo(quantumfs.ZeroKey) {
		newerAttrs = emptyAttrs
	} else if err != fuse.OK {
		return quantumfs.EmptyBlockKey, errors.New("Merge ExtAttr new: " +
			err.String())
	}

	olderAttrs, err := getRecordExtendedAttributes(c, older)
	if err == fuse.ENOENT || older.IsEqualTo(quantumfs.ZeroKey) {
		olderAttrs = emptyAttrs
	} else if err != fuse.OK {
		return quantumfs.EmptyBlockKey, errors.New("Merge ExtAttr old: " +
			err.String())
	}

	mergeAttrs := quantumfs.NewExtendedAttributes()

	// Add new attrs, but only if they weren't removed in the older branch
	for i := 0; i < newerAttrs.NumAttributes(); i++ {
		key, newerId := newerAttrs.Attribute(i)
		olderId := olderAttrs.AttributeByKey(key)

		if baseAttrs != nil {
			baseId := baseAttrs.AttributeByKey(key)
			// skip this attribute since it was removed
			if baseId != quantumfs.EmptyBlockKey &&
				olderId == quantumfs.EmptyBlockKey {

				continue
			}
		}

		mergeAttrs.SetAttribute(mergeAttrs.NumAttributes(), key, newerId)
		mergeAttrs.SetNumAttributes(mergeAttrs.NumAttributes() + 1)
	}

	// Add attrs that were added or only changed by the older branch
	for i := 0; i < olderAttrs.NumAttributes(); i++ {
		key, olderId := olderAttrs.Attribute(i)
		newerId := newerAttrs.AttributeByKey(key)

		setId := false

		if baseAttrs != nil {
			baseId := baseAttrs.AttributeByKey(key)
			if (baseId == quantumfs.EmptyBlockKey &&
				newerId == quantumfs.EmptyBlockKey) ||
				(baseId == newerId) {

				setId = true
			}
		} else if newerId == quantumfs.EmptyBlockKey {
			setId = true
		}

		if setId {
			// Take the diff from older
			mergeAttrs.SetAttribute(mergeAttrs.NumAttributes(), key,
				olderId)
			mergeAttrs.SetNumAttributes(mergeAttrs.NumAttributes() + 1)
		}
	}

	// Publish the result
	buffer := newBuffer(c, mergeAttrs.Bytes(), quantumfs.KeyTypeMetadata)
	rtnKey, bufErr := buffer.Key(&c.Ctx)
	if bufErr != nil {
		c.elog("Error computing extended attribute key: %v", bufErr.Error())
		return quantumfs.EmptyBlockKey, bufErr
	}

	return rtnKey, nil
}

// Merge record attributes based on ContentTime
func mergeAttributes(c *ctx, base quantumfs.DirectoryRecord,
	remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) (quantumfs.DirectoryRecord, error) {

	newerRecord := local
	olderRecord := remote
	if remote.ContentTime() > local.ContentTime() {
		newerRecord = remote
		olderRecord = local
	}

	rtnRecord := newerRecord.Clone()

	if base == nil {
		// Without a base we cannot be any cleverer than newest wins.
		return rtnRecord, nil
	}

	if local.FileId() != remote.FileId() {
		// At least one of the sides replaced the base as a deletion followed
		// by a creation. Stay consistent with the deletion-modification
		// scenario and keep the newly created file.
		if local.FileId() == base.FileId() {
			// The remote recreated
			return remote.Clone(), nil
		} else if remote.FileId() == base.FileId() {
			// The local recreated
			return local.Clone(), nil
		} else {
			// Both recreated, keep the most recently created
			return newerRecord.Clone(), nil
		}
	} else {
		// local.FileId() == remote.FileId()
		//
		// We only take fields from the older record when the newer record and base
		// have the same value, indicating no change from that branch

		if base.ID() == newerRecord.ID() {
			rtnRecord.SetID(olderRecord.ID())
			// type and size must match the content set via ID
			rtnRecord.SetSize(olderRecord.Size())
			rtnRecord.SetType(olderRecord.Type())
		}
		if base.Permissions() == newerRecord.Permissions() {
			rtnRecord.SetPermissions(olderRecord.Permissions())
		}
		if base.Owner() == newerRecord.Owner() {
			rtnRecord.SetOwner(olderRecord.Owner())
		}
		if base.Group() == newerRecord.Group() {
			rtnRecord.SetGroup(olderRecord.Group())
		}

		newKey, err := mergeExtendedAttrs(c, base.ExtendedAttributes(),
			newerRecord.ExtendedAttributes(),
			olderRecord.ExtendedAttributes())
		if err != nil {
			return nil, err
		}
		rtnRecord.SetExtendedAttributes(newKey)

		if base.ContentTime() == newerRecord.ContentTime() {
			rtnRecord.SetContentTime(olderRecord.ContentTime())
		}
		if base.ModificationTime() == newerRecord.ModificationTime() {
			rtnRecord.SetModificationTime(0 +
				olderRecord.ModificationTime())
		}
	}

	return rtnRecord, nil
}

func mergeRecord(c *ctx, base quantumfs.DirectoryRecord,
	remote quantumfs.DirectoryRecord, local quantumfs.DirectoryRecord,
	ht *hardlinkTracker) (quantumfs.DirectoryRecord, error) {

	defer c.FuncIn("mergeRecord", "%s", local.Filename()).Out()

	// Merge differently depending on if the type is preserved
	localTypeChanged := base == nil || !local.Type().Matches(base.Type())
	remoteTypeChanged := base == nil || !remote.Type().Matches(base.Type())
	bothSameType := local.Type().Matches(remote.Type())

	rtnRecord, err := mergeAttributes(c, base, remote, local)
	if err != nil {
		return nil, err
	}

	switch local.Type() {
	case quantumfs.ObjectTypeDirectory:
		if (!localTypeChanged && !remoteTypeChanged) ||
			(base == nil && bothSameType) {

			var baseId quantumfs.ObjectKey
			if base != nil {
				baseId = base.ID()
			}

			mergedKey, err := mergeDirectory(c, baseId,
				remote.ID(), local.ID(), (base != nil), ht)
			if err != nil {
				return local, err
			}

			rtnRecord.SetID(mergedKey)
		}
	case quantumfs.ObjectTypeHardlink:
		if bothSameType {
			// hardlinks use ContentTime to store their created timestamp
			if remote.ContentTime() > local.ContentTime() {
				c.vlog("taking remote copy of %s", remote.Filename())
				return remote, nil
			}

			c.vlog("keeping local copy of %s", remote.Filename())
			return local, nil
		}
	case quantumfs.ObjectTypeSmallFile:
		fallthrough
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		fallthrough
	case quantumfs.ObjectTypeVeryLargeFile:
		if bothSameType {
			// We can potentially do an intra-file merge
			return mergeFile(c, base, remote, local)
		}
	}

	// If one of them is a hardlink, we have to handle the situation differently
	if local.Type() == quantumfs.ObjectTypeHardlink ||
		remote.Type() == quantumfs.ObjectTypeHardlink {

		hardlink := local
		if remote.Type() == quantumfs.ObjectTypeHardlink {
			hardlink = remote
		}

		// If the FileIds match, just take the hardlink to "convert" the file
		if local.FileId() != remote.FileId() {
			// Check in case this hardlink leg was overwritten
			if remote.Type() != quantumfs.ObjectTypeHardlink &&
				local.ContentTime() < remote.ModificationTime() {

				return remote, nil
			}

			if local.Type() != quantumfs.ObjectTypeHardlink &&
				remote.ContentTime() < local.ModificationTime() {

				return local, nil
			}
		}

		return hardlink, nil
	}

	// if we took remote for a directory, we have to accommodate its
	// hardlinks in the hardlink tracker
	if rtnRecord.ID() == remote.ID() && remote.ID() != local.ID() &&
		remote.Type() == quantumfs.ObjectTypeDirectory {

		// Add new links
		err = traverseSubtree(c, remote.ID(),
			func(v quantumfs.DirectoryRecord) {

				ht.checkLinkChanged(c, nil, v)
			})
		if err != nil {
			return nil, err
		}

		// Remove old links
		err = traverseSubtree(c, local.ID(),
			func(v quantumfs.DirectoryRecord) {

				ht.checkLinkChanged(c, v, nil)
			})
		if err != nil {
			return nil, err
		}
	}

	return rtnRecord, nil
}

func loadAccessor(c *ctx, record quantumfs.DirectoryRecord) blockAccessor {
	switch record.Type() {
	case quantumfs.ObjectTypeSmallFile:
		return newSmallAccessor(c, record.Size(), record.ID())
	case quantumfs.ObjectTypeMediumFile:
		return newMediumAccessor(c, record.ID())
	case quantumfs.ObjectTypeLargeFile:
		return newLargeAccessor(c, record.ID())
	case quantumfs.ObjectTypeVeryLargeFile:
		return newVeryLargeAccessor(c, record.ID())
	}

	return nil
}

func chooseAccessors(c *ctx, remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) (iterator blockAccessor,
	iteratorRecord quantumfs.DirectoryRecord, other blockAccessor,
	otherRecord quantumfs.DirectoryRecord) {

	localAccessor := loadAccessor(c, local)
	remoteAccessor := loadAccessor(c, remote)

	iteratorRecord = local
	iterator = localAccessor
	otherRecord = remote
	other = remoteAccessor
	if localAccessor.fileLength(c) > remoteAccessor.fileLength(c) {
		iteratorRecord = remote
		iterator = remoteAccessor
		otherRecord = local
		other = localAccessor
	}

	return iterator, iteratorRecord, other, otherRecord
}

func mergeFile(c *ctx, base quantumfs.DirectoryRecord,
	remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) (quantumfs.DirectoryRecord, error) {

	var baseAccessor blockAccessor
	baseAvailable := false
	if base != nil && base.Type().IsRegularFile() {
		baseAccessor = loadAccessor(c, base)
		baseAvailable = true
	}

	rtnRecord, err := mergeAttributes(c, base, remote, local)
	if err != nil {
		return nil, err
	}

	iterator, iteratorRecord, other, otherRecord := chooseAccessors(c, remote,
		local)

	if iterator != nil && other != nil &&
		local.FileId() == remote.FileId() &&
		local.Type().IsRegularFile() && remote.Type().IsRegularFile() {

		// Perform an intra-file merge by iterating through the shorter file,
		// writing its changes to other, and then keeping other
		otherIsOlder := (iteratorRecord.ModificationTime() >
			otherRecord.ModificationTime())

		baseBuf := make([]byte, quantumfs.MaxBlockSize)
		iterBuf := make([]byte, quantumfs.MaxBlockSize)
		otherBuf := make([]byte, quantumfs.MaxBlockSize)

		// iterate through the smaller accessor so we don't have to handle
		// reconciling the accessor type - the size won't change this way
		operateOnBlocks(c, iterator, 0, uint32(other.fileLength(c)),
			func(c *ctx, blockIdx int, offset uint64) error {
				var err error
				baseRead := 0
				if baseAvailable {
					baseRead, err = baseAccessor.readBlock(c,
						blockIdx, offset, baseBuf)
					if err != nil {
						return err
					}
				}

				iteratorRead, err := iterator.readBlock(c, blockIdx,
					offset, iterBuf)
				if err != nil {
					return err
				}

				otherRead, err := other.readBlock(c, blockIdx,
					offset, otherBuf)
				if err != nil {
					return err
				}

				utils.Assert(iteratorRead <= otherRead,
					"smaller file has more data somehow")

				// merge each buffer byte by byte, where
				// we take the iterator byte if either:
				// 1) there is no base reference and other is older
				// 2) there is a base reference and other matches it
				// 3) there is a base ref, but it matches neither and
				//    other is older
				baseRefCount := baseRead
				if iteratorRead < baseRead {
					baseRefCount = iteratorRead
				}
				for i := 0; i < baseRefCount; i++ {
					if otherBuf[i] == baseBuf[i] ||
						(otherIsOlder &&
							otherBuf[i] != baseBuf[i] &&
							iterBuf[i] != baseBuf[i]) {

						otherBuf[i] = iterBuf[i]
					}
				}

				for i := baseRead; i < iteratorRead; i++ {
					if otherIsOlder {
						otherBuf[i] = iterBuf[i]
					}
				}

				_, err = other.writeBlock(c, blockIdx,
					offset, otherBuf[:otherRead])
				if err != nil {
					return err
				}

				return nil
			})

		// Use the merged record as a base and update content relevant fields
		rtnRecord.SetType(otherRecord.Type())
		rtnRecord.SetSize(other.fileLength(c))
		rtnRecord.SetID(other.sync(c))

		c.vlog("Merging file contents for %d %s", local.FileId(),
			local.Filename())
		return rtnRecord, nil
	}

	c.vlog("File conflict for %s resulting in overwrite. %d %d",
		local.Filename(), local.FileId(), remote.FileId())

	return rtnRecord, nil
}
