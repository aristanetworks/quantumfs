// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to special.go

import "encoding/binary"

import "github.com/aristanetworks/quantumfs"

func (special *Special) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	special.setDirty(false)
	var hash [quantumfs.ObjectKeyLength - 1]byte

	binary.LittleEndian.PutUint32(hash[0:4], special.filetype)
	binary.LittleEndian.PutUint32(hash[4:8], special.device)

	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, hash)
}
