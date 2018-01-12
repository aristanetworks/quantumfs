// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
)

func freshenKeys(c *ctx, key quantumfs.ObjectKey,
	type_ quantumfs.ObjectType) bool {

	defer c.FuncIn("daemon::freshenKeys", "%d", type_).Out()

	switch type_ {
	default:
		c.elog("Unsupported type for key freshen: %d", type_)
		return false
	case quantumfs.ObjectTypeSmallFile:
		return freshenSmallFile(c, key)
	case quantumfs.ObjectTypeLargeFile:
		fallthrough
	case quantumfs.ObjectTypeMediumFile:
		return freshenMultiBlock(c, key)
	case quantumfs.ObjectTypeVeryLargeFile:
		return freshenVeryLargeFile(c, key)
	case quantumfs.ObjectTypeSymlink:
		return freshenSymlink(c, key)
	case quantumfs.ObjectTypeSpecial:
		// nothing to do for embedded keys
		return true
	}
}

func freshenSmallFile(c *ctx, key quantumfs.ObjectKey) bool {
	_, success := c.dataStore.Freshen(c, key)
	return success
}

func freshenMultiBlock(c *ctx, key quantumfs.ObjectKey) bool {
	buf, success := c.dataStore.Freshen(c, key)
	if !success {
		return false
	}

	store := buf.AsMultiBlockFile()
	for _, block := range store.ListOfBlocks() {
		_, success = c.dataStore.Freshen(c, block)
		if !success {
			return false
		}
	}

	return true
}

func freshenVeryLargeFile(c *ctx, key quantumfs.ObjectKey) bool {
	buf, success := c.dataStore.Freshen(c, key)
	if !success {
		return false
	}

	store := buf.AsVeryLargeFile()
	for i := 0; i < store.NumberOfParts(); i++ {
		success = freshenMultiBlock(c, store.LargeFileKey(i))
		if !success {
			return false
		}
	}

	return true
}

func freshenSymlink(c *ctx, key quantumfs.ObjectKey) bool {
	_, success := c.dataStore.Freshen(c, key)
	return success
}
