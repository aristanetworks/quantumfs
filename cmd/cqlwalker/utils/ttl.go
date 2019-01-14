// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

import (
	"fmt"
	"strconv"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/walker"
)

// RefreshTTL will update the TTL of the block pointed to by key.
func RefreshTTL(c *walker.Ctx, path string, key quantumfs.ObjectKey,
	size uint64, objType quantumfs.ObjectType, cqlds cql.BlobStore, newTTL int64,
	ttlThreshold int64, globalSkipMap *SkipMap, localSkipMap *SkipMap) error {

	if walker.SkipKey(c, key) {
		return nil
	}

	kv := key.Value()
	ks := key.String()

	// Check to see if the key is present in either the globalSkipMap or the
	// localSkipMap. localSkipMap is just specific to this workspaces's walk.
	if globalSkipMap != nil && globalSkipMap.Check(ks) {
		return walker.ErrSkipHierarchy
	}

	if localSkipMap != nil && localSkipMap.Check(ks) {
		return walker.ErrSkipHierarchy
	}

	if globalSkipMap == nil {
		// legacy behavior
		ttlVal, err := GetTTLForKey(c, cqlds, key, path)
		if err != nil {
			return fmt.Errorf("refreshTTL: %v", err)
		}

		// if key exists and TTL doesn't need to be refreshed
		// then return.
		if ttlVal >= ttlThreshold {
			return nil
		}
	}

	buf, _, err := cqlds.Get(ToECtx(c), kv)
	if err != nil {
		return fmt.Errorf("refreshTTL: path: %v key %v: %v", path, ks, err)
	}
	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", newTTL)
	err = cqlds.Insert(ToECtx(c), kv, buf, newmetadata)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, ks, err)
	}

	// On successfully refreshing the TTL, add the key to localSkipMap.
	if localSkipMap != nil {
		localSkipMap.Set(ks)
		// If there is no localSkipMap, add it to the globalSkipMap
	} else if globalSkipMap != nil {
		globalSkipMap.Set(ks)
	}
	return nil
}

//GetTTLForKey return the the TTL of the block
func GetTTLForKey(c *walker.Ctx, cqlds cql.BlobStore,
	key quantumfs.ObjectKey, path string) (int64, error) {

	metadata, err := cqlds.Metadata(ToECtx(c), key.Value())
	if err != nil {
		return 0, fmt.Errorf("Metadata failed on path:%v key %v: %v",
			path, key.String(), err)
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
