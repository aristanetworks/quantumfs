// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"fmt"
	"strconv"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/walker"
)

// RefreshTTL will update the TTL of the block pointed to by key.
func RefreshTTL(c *walker.Ctx, path string, key quantumfs.ObjectKey,
	size uint64, isDir bool, cqlds blobstore.BlobStore, newTTL int64,
	skipMap *SkipMap) error {

	if walker.SkipKey(c, key) {
		return nil
	}

	if skipMap != nil && skipMap.Check(c, key) {
		return walker.SkipEntry
	}

	kv := key.Value()
	buf, _, err := cqlds.Get(ToECtx(c), kv)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, key.String(), err)
	}

	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", newTTL)
	err = cqlds.Insert(ToECtx(c), kv, buf, newmetadata)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, key.String(), err)
	}

	// only set the skipMap if we return successfully
	if skipMap != nil {
		skipMap.Set(c, key)
	}
	return nil
}

//GetTTLForKey return the the TTL of the block
func GetTTLForKey(c *walker.Ctx, cqlds blobstore.BlobStore,
	key quantumfs.ObjectKey, path string) (int64, error) {

	metadata, err := cqlds.Metadata(ToECtx(c), key.Value())
	if err != nil {
		return 0, fmt.Errorf("Metadata failed on path:%v key %v: %v", path, key.String(), err)
	}
	ttl, ok := metadata[cql.TimeToLive]
	if !ok {
		return 0, fmt.Errorf("Store must return metadata with " +
			"TimeToLive")
	}
	ttlVal, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Invalid TTL value in metadata %s ",
			ttl)
	}
	return ttlVal, nil
}
