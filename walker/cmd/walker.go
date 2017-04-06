// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/walker"

func main() {

	ZeroKey := quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded,
		[quantumfs.ObjectKeyLength - 1]byte{})
	_ = walker.Walk(nil, nil, ZeroKey, nil)
}
