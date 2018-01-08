// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
)

func refreshKeyTTLs(c *ctx, key quantumfs.ObjectKey,
	type_ quantumfs.ObjectType) bool {

	switch type_ {
		default:
			c.elog("Unsupported type for key TTL refresh: %d", type_)
			return false
		case quantumfs.ObjectTypeSmallFile:
			return refreshSmallFileTTL(c, key)
		case quantumfs.ObjectTypeLargeFile:
			fallthrough
		case quantumfs.ObjectTypeMediumFile:
			return refreshMultiBlockTTL(c, key)
		case quantumfs.ObjectTypeVeryLargeFile:
			return refreshVeryLargeFileTTL(c, key)
		case quantumfs.ObjectTypeSymlink:
			return refreshSymlinkTTL(c, key)
		case quantumfs.ObjectTypeSpecial:
			// nothing to do for embedded keys
			return true
	}
}

func refreshTTL(c *ctx, key quantumfs.ObjectKey) (quantumfs.Buffer, bool) {
	buf := c.dataStore.Get(&c.Ctx, key)
	if buf == nil {
		return nil, false
	}

	err := c.dataStore.durableStore.Set(&c.Ctx, key, buf)
	return buf, err == nil
}

func refreshSmallFileTTL(c *ctx, key quantumfs.ObjectKey) bool {
	_, success := refreshTTL(c, key)
	return success
}

func refreshMultiBlockTTL(c *ctx, key quantumfs.ObjectKey) bool {
	buf, success := refreshTTL(c, key)
	if !success {
		return false
	}

	store := buf.AsMultiBlockFile()
	for _, block := range store.ListOfBlocks() {
		_, success = refreshTTL(c, block)
		if !success {
			return false
		}
	}

	return true
}

func refreshVeryLargeFileTTL(c *ctx, key quantumfs.ObjectKey) bool {
	buf, success := refreshTTL(c, key)
	if !success {
		return false
	}

	store := buf.AsVeryLargeFile()
	for i := 0; i < store.NumberOfParts(); i++ {
		success = refreshMultiBlockTTL(c, store.LargeFileKey(i))
		if !success {
			return false
		}
	}

	return true
}

func refreshSymlinkTTL(c *ctx, key quantumfs.ObjectKey) bool {
	_, success := refreshTTL(c, key)
	return success
}
