// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

// By successfully building this dummy command based
// on walker library we ensure that there are no
// accidental test dependencies included in the library
// code.
//
// This command is not expected to be successfully runnable.

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/walker"
)

func main() {

	ZeroKey := quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded,
		[quantumfs.ObjectKeyLength - 1]byte{})
	_ = walker.Walk(nil, nil, ZeroKey, nil)
}
