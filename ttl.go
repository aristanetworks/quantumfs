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
// If the current TTL of the block is less than thresholdTTL, it
// will be updated to newTTL.
func RefreshTTL(c *walker.Ctx, path string, key quantumfs.ObjectKey,
	size uint64, isDir bool, cqlds blobstore.BlobStore,
	thresholdTTL int64, newTTL int64) error {

	if walker.SkipKey(c, key) {
		return nil
	}

	ks := key.String()
	metadata, err := cqlds.Metadata(ToECtx(c), ks)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
	}
	if metadata == nil {
		return fmt.Errorf("Store must have metadata")
	}
	ttl, ok := metadata[cql.TimeToLive]
	if !ok {
		return fmt.Errorf("Store must return metadata with TimeToLive")
	}
	ttlVal, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil {
		return fmt.Errorf("Invalid TTL value in metadata %s ", ttl)
	}

	// if key exists and TTL doesn't need to be refreshed
	// then return.
	if ttlVal >= thresholdTTL {
		return nil
	}

	buf, _, err := cqlds.Get(ToECtx(c), ks)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
	}

	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", newTTL)
	err = cqlds.Insert(ToECtx(c), ks, buf, newmetadata)
	if err != nil {
		return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
	}
	return nil
}
