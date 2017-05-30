// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"

import "github.com/aristanetworks/quantumfs"

func loadWorkspaceRoot(c *ctx,
	key quantumfs.ObjectKey) (hardlinks map[HardlinkId]linkEntry,
	nextHardlinkId HardlinkId, directory quantumfs.ObjectKey, err error) {

	buffer := c.dataStore.Get(&c.Ctx, key)
	if buffer == nil {
		return nil, HardlinkId(0), key,
			fmt.Errorf("Unable to Get block for key: %s", key.String())
	}
	workspaceRoot := buffer.AsWorkspaceRoot()

	links, nextId := loadHardlinks(c, workspaceRoot.HardlinkEntry())

	return links, nextId, workspaceRoot.BaseLayer(), nil
}

func mergeWorkspaceRoot(c *ctx, base quantumfs.ObjectKey, remote quantumfs.ObjectKey,
	local quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.funcIn("mergeWorkspaceRoot").Out()

	baseHardlinks, _, baseDirectory, err := loadWorkspaceRoot(c, base)
	if err != nil {
		return local, err
	}
	remoteHardlinks, _, remoteDirectory, err := loadWorkspaceRoot(c, remote)
	if err != nil {
		return local, err
	}
	localHardlinks, nextHardlinkId, localDirectory, err := loadWorkspaceRoot(c,
		local)
	if err != nil {
		return local, err
	}

	// We assume that local and remote are newer versions of base
	for k, v := range remoteHardlinks {
		_, baseExists := baseHardlinks[k]
		localLink, localExists := localHardlinks[k]

		if localExists {
			toSet, setId := mergeLink(c, baseExists, v, localLink,
				&nextHardlinkId, k)
			localHardlinks[setId] = toSet
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
	nextHardlinkId *HardlinkId, localId HardlinkId) (toSet linkEntry,
	setId HardlinkId) {

	// If there is no base entry, that means that these hardlinks were
	// both created independently and aren't related. Separate them by setting
	// the remote with a new hardlink id
	if !baseExists {
		rtnId := *nextHardlinkId
		*nextHardlinkId++

		return remote, rtnId
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

	return rtn, localId
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
