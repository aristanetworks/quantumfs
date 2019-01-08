// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package aggregatedatastore

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
)

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

		// Check for the constant key
		// in the durable datastore
		key := quantumfs.EmptyDirKey
		buf := simplebuffer.New(nil, key)
		err := th.ds.Get(th.testCtx(), key, buf)
		th.Assert(err != nil, "Get should have failed")

		// Nothing is Set in the dataStore,
		// Still this Get should succeed
		// for a constant key.
		buf = simplebuffer.New(nil, key)
		err = ads.Get(th.testCtx(), key, buf)
		th.Assert(err == nil, "Get failed")

		// Expect from DataStore something
		// which has not been Set.
		// Since it is not a constant key it should fail.
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
