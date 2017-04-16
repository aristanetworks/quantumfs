// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/utils/simplebuffer"

func writeBlock(qctx *quantumfs.Ctx, data []byte, keyType quantumfs.KeyType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	key := quantumfs.NewObjectKey(keyType, hash.Hash(data))
	buf := simplebuffer.New(data, key)
	return key, ds.Set(qctx, key, buf)
}
