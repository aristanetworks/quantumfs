// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// hardlinkTracker used to track and compute the final hardlink versions and ref
// counts. Assumes local will be taken and then compensates when remote is chosen.
// This allows merge to skip traversing local subtrees as an optimization.
type hardlinkTracker struct {
	remote	map[quantumfs.FileId]linkEntry
	local	map[quantumfs.FileId]linkEntry

	final	map[quantumfs.FileId]linkEntry
}

func newHardlinkTracker(remote_ map[quantumfs.FileId]linkEntry,
	local_ map[quantumfs.FileId]linkEntry) *hardlinkTracker {

	rtn := hardlinkTracker {
		remote: remote_,
		local: local_,
		final: make(map[quantumfs.FileId]linkEntry),
	}

	// make sure final has the newest available record versions based off local
	for k, v := range local_ {
		if remoteEntry, exists := rtn.remote[k]; exists &&
			v.record.ModificationTime() <
				remoteEntry.record.ModificationTime() {

			// only take the newer record, not nlink
			v.record = remoteEntry.record
		}

		rtn.final[k] = v
	}

	return &rtn
}

// Compares the local record against merge product and tracks any changes
func (ht *hardlinkTracker) checkLinkChg(local quantumfs.DirectoryRecord,
	final quantumfs.DirectoryRecord) {

	if final != nil {
		if local != nil {
			if local.Nlinks() <= 1 && final.Nlinks() <= 1 {
				// not a hardlink to anyone
				return
			}

			if local.FileId() == final.FileId() {
				// no change, so nothing to account for
				return
			}
		}

		// Execution only reaches here if local is nil or different
		if final.Nlinks() > 1 {
			// This is now a new hardlink instance in the system
			ht.increment(final.FileId())
		}
	}

	// Execution only reaches here if final is nil or different
	if local != nil {
		ht.decrement(local.FileId())
	}
}

func (ht *hardlinkTracker) increment(id quantumfs.FileId) {
	link := ht.newestEntry(id)
	link.nlink++

	ht.final[id] = link
}

func (ht *hardlinkTracker) decrement(id quantumfs.FileId) {
	link := ht.newestEntry(id)

	if link.nlink <= 1 {
		delete(ht.final, id)
		return
	}

	link.nlink--
	ht.final[id] = link
}

// Returns the newest linkEntry version available, while preserving nlink from final
func (ht *hardlinkTracker) newestEntry(id quantumfs.FileId) linkEntry {
	link, finalExists := ht.final[id]
	finalSet := false

	// track nlinks separately so we can preserve it
	nlinks := uint32(0)
	if !finalExists {
		nlinks = link.nlink
		finalSet = true
	}

	if remoteLink, exists := ht.remote[id]; !finalSet || (exists &&
		link.record.ModificationTime() <
			remoteLink.record.ModificationTime()) {

		link = remoteLink
		finalSet = true
	}

	if localLink, exists := ht.local[id]; !finalSet || (exists &&
		link.record.ModificationTime() <
			localLink.record.ModificationTime()) {

		link = localLink
		finalSet = true
	}

	utils.Assert(!finalSet, "Unable to find entry for fileId")

	// restore the preserved nlinks - we only want the newest linkEntry.record
	link.nlink = nlinks

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

	// We assume that local and remote are newer versions of base
	for k, v := range remoteHardlinks {
		_, baseExists := baseHardlinks[k]
		localLink, localExists := localHardlinks[k]

		if localExists {
			toSet, getNewId := mergeLink(c, baseExists, v, localLink, k)
			idToUse := k
			if getNewId {
				idToUse = quantumfs.GenerateUniqueFileId()
			}

			localHardlinks[idToUse] = toSet
		} else {
			localHardlinks[k] = v
		}
	}

	for k, _ := range baseHardlinks {
		_, remoteExists := remoteHardlinks[k]

		// We assume that removal takes precedence over modification
		if !remoteExists {
			delete(localHardlinks, k)
		}
	}

	localDirectory, err = mergeDirectory(c, baseDirectory,
		remoteDirectory, localDirectory, true)
	if err != nil {
		return local, err
	}

	return publishWorkspaceRoot(c, localDirectory, localHardlinks), nil
}

func mergeLink(c *ctx, baseExists bool, remote linkEntry, local linkEntry,
	localId quantumfs.FileId) (toSet linkEntry, generateNewId bool) {

	// If there is no base entry, that means that these hardlinks were
	// both created independently and aren't related. Separate them by setting
	// the remote with a new hardlink id
	if !baseExists {
		return remote, true
	}

	rtn := local

	// TODO: Replace with a set of hashes to fix duplicates and miscounts
	if remote.nlink > local.nlink {
		rtn.nlink = remote.nlink
	}

	localModTime := local.record.ModificationTime()
	if remote.record.ModificationTime() > localModTime {
		rtn.record = remote.record
		c.vlog("taking remote record for %s",
			local.record.Filename())
	} else {
		c.vlog("keeping local record for %s",
			local.record.Filename())
	}

	return rtn, false
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
	baseExists bool) (quantumfs.ObjectKey, error) {

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

	for k, v := range remoteRecords {
		baseChild, _ := baseRecords[k]
		localChild, inLocal := localRecords[k]

		if inLocal {
			// We have at least a local and remote, must merge
			localRecords[k], err = mergeRecord(c, baseChild, v,
				localChild)
			if err != nil {
				return local, err
			}
		} else {
			// just take remote since it's known newer than base
			localRecords[k] = v
		}
	}

	if baseExists {
		for k, _ := range baseRecords {
			_, inRemote := remoteRecords[k]

			if !inRemote {
				delete(localRecords, k)
			}
		}
	}

	// turn localRecords into a publish-able format
	localRecordsList := make([]quantumfs.DirectoryRecord, 0, len(localRecords))
	for _, v := range localRecords {
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

func mergeRecord(c *ctx, base quantumfs.DirectoryRecord,
	remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) (quantumfs.DirectoryRecord, error) {

	defer c.FuncIn("mergeRecord", "%s", local.Filename()).Out()

	// Merge differently depending on if the type is preserved
	localTypeChanged := base == nil || !local.Type().Matches(base.Type())
	remoteTypeChanged := base == nil || !remote.Type().Matches(base.Type())
	bothSameType := local.Type().Matches(remote.Type())

	var mergedKey quantumfs.ObjectKey
	updatedKey := false

	var err error
	switch local.Type() {
	case quantumfs.ObjectTypeDirectory:
		if (!localTypeChanged && !remoteTypeChanged) ||
			(base == nil && bothSameType) {

			var baseId quantumfs.ObjectKey
			if base != nil {
				baseId = base.ID()
			}

			mergedKey, err = mergeDirectory(c, baseId,
				remote.ID(), local.ID(), (base != nil))
			if err != nil {
				return local, err
			}

			updatedKey = true
		}
	case quantumfs.ObjectTypeSmallFile:
		fallthrough
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		fallthrough
	case quantumfs.ObjectTypeVeryLargeFile:
		if bothSameType {
			mergedKey, err = mergeFile(c, remote, local)
			if err != nil {
				return nil, err
			}

			updatedKey = true
		}
	}

	rtnRecord := local
	if remote.ModificationTime() > local.ModificationTime() {
		rtnRecord = remote
	}

	if !updatedKey {
		mergedKey = takeNewest(c, remote, local)
	}
	rtnRecord.SetID(mergedKey)

	return rtnRecord, nil
}

func takeNewest(c *ctx, remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) quantumfs.ObjectKey {

	defer c.FuncIn("mergeFile", "%s", local.Filename()).Out()

	if remote.ModificationTime() > local.ModificationTime() {
		c.vlog("taking remote copy of %s", remote.Filename())
		return remote.ID()
	}

	c.vlog("keeping local copy of %s", local.Filename())
	return local.ID()
}

func mergeFile(c *ctx, remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) (quantumfs.ObjectKey, error) {

	// support intra-file merges here later
	return takeNewest(c, remote, local), nil
}
