// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package aggregatedatastore

import "testing"
import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils/simplebuffer"

var requestId uint64

func TestAggDSNew(t *testing.T) {
	runTest(t, func(th *testHelper) {
		ads := New(th.ds)
		th.Assert(ads != nil, "New failed")
	})
}

func TestAggDSGetSet(t *testing.T) {
	runTest(t, func(th *testHelper) {

		ads := New(th.ds)
		th.Assert(ads != nil, "New failed")

		// Nothing is Set in the dataStore,
		// Still this Get should succeed
		// for a constant key.
		key := quantumfs.EmptyDirKey
		buf := simplebuffer.New(nil, key)
		err := ads.Get(th.testCtx(), key, buf)
		th.Assert(err == nil, "Get failed")

		// Expect from DataStore something
		// which has not been Set.
		// Since not a constant key it should fail.
		data := []byte("Resistance is futile !!")
		var hash [quantumfs.HashSize]byte
		copy(hash[:len(hash)], data)
		key = quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata, hash)
		err = ads.Get(th.testCtx(), key, buf)
		th.Assert(err != nil, "Get should have failed")

		// Then Set the Key and check Get again.
		err = ads.Set(th.testCtx(), key, buf)
		th.Assert(err == nil, "Set failed")

		err = ads.Get(th.testCtx(), key, buf)
		th.Assert(err == nil, "Get failed")
	})
}
