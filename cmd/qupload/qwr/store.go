// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qwr

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/hash"
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
)

func writeBlock(qctx *quantumfs.Ctx, data []byte, keyType quantumfs.KeyType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	key := quantumfs.NewObjectKey(keyType, hash.Hash(data))
	buf := simplebuffer.New(data, key)
	return key, ds.Set(qctx, key, buf)
}
