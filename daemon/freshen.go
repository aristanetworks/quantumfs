// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
)

func freshenKeys(c *ctx, key quantumfs.ObjectKey,
	type_ quantumfs.ObjectType) error {

	defer c.FuncIn("daemon::freshenKeys", "%d", type_).Out()

	switch type_ {
	default:
		c.elog("Unsupported type for key freshen: %d", type_)
		return nil
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
		return nil
	}
}

func freshenSmallFile(c *ctx, key quantumfs.ObjectKey) error {
	_, err := c.dataStore.Freshen(c, key)
	return err
}

func freshenMultiBlock(c *ctx, key quantumfs.ObjectKey) error {
	buf, err := c.dataStore.Freshen(c, key)
	if err != nil {
		return err
	}

	store := buf.AsMultiBlockFile()
	for _, block := range store.ListOfBlocks() {
		_, err = c.dataStore.Freshen(c, block)
		if err != nil {
			return err
		}
	}

	return nil
}

func freshenVeryLargeFile(c *ctx, key quantumfs.ObjectKey) error {
	buf, err := c.dataStore.Freshen(c, key)
	if err != nil {
		return err
	}

	store := buf.AsVeryLargeFile()
	for i := 0; i < store.NumberOfParts(); i++ {
		err = freshenMultiBlock(c, store.LargeFileKey(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func freshenSymlink(c *ctx, key quantumfs.ObjectKey) error {
	_, err := c.dataStore.Freshen(c, key)
	return err
}
