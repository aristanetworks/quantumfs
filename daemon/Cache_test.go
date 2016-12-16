// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the internal datastore cache

import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/qlog"

type testDataStore struct {
	datastore  quantumfs.DataStore
	shouldRead bool
	test       *testHelper
}

func newTestDataStore(test *testHelper) *testDataStore {
	return &testDataStore{
		datastore:  processlocal.NewDataStore(""),
		shouldRead: true,
		test:       test,
	}
}

func (store *testDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	store.test.assert(store.shouldRead, "Received unexpected Get for %s",
		key.String())
	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.datastore.Set(c, key, buf)
}

func primeDatastore(c *quantumfs.Ctx, test *testHelper, backingStore *testDataStore,
	datastore *dataStore, cacheSize int,
	keys map[int]quantumfs.ObjectKey) {

	for i := 1; i < 2*cacheSize; i++ {
		bytes := make([]byte, quantumfs.ObjectKeyLength)
		bytes[1] = byte(i % 256)
		bytes[2] = byte(i / 256)
		key := quantumfs.NewObjectKeyFromBytes(bytes)
		keys[i] = key
		buf := &buffer{
			data:      bytes,
			dirty:     false,
			keyType:   quantumfs.KeyTypeData,
			key:       key,
			dataStore: datastore,
		}
		err := backingStore.Set(c, key, buf)
		test.assert(err == nil, "Error priming datastore: %v", err)
	}
}

func TestCacheLru(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		cacheSize := 256
		backingStore := newTestDataStore(test)
		datastore := newDataStore(backingStore, cacheSize)

		keys := make(map[int]quantumfs.ObjectKey, 2*cacheSize)

		ctx := ctx{
			Ctx: quantumfs.Ctx{
				Qlog:      test.logger,
				RequestId: qlog.TestReqId,
			},
		}
		c := &ctx.Ctx

		primeDatastore(c, test, backingStore, datastore, cacheSize, keys)

		// Prime the LRU by reading every entry in reverse order. At the end
		// we should have the first cacheSize elements in the cache.
		test.log("Priming LRU")
		for i := 2*cacheSize - 1; i > 0; i-- {
			buf := datastore.Get(c, keys[i])
			test.assert(buf != nil, "Failed retrieving block %d", i)
		}
		test.log("Verifying cache")
		test.assert(len(datastore.cache) == cacheSize,
			"Cache size incorrect %d != %d", len(datastore.cache),
			cacheSize)
		for _, v := range datastore.cache {
			i := int(v.data[1]) + int(v.data[2])*256
			test.assert(i <= cacheSize+1,
				"Unexpected block in cache %d", i)
		}
		test.log("Verifying LRU")
		test.assert(datastore.lru.Len() == cacheSize,
			"Lru size incorrect %d != %d", datastore.lru.Len(),
			cacheSize)
		num := 1
		for e := datastore.lru.Back(); e != nil; e = e.Prev() {
			buf := e.Value.(buffer)
			i := int(buf.data[1]) + int(buf.data[2])*256
			test.assert(i <= cacheSize+1,
				"Unexpected block in lru %d", i)
			test.assert(i == num, "Out of order block %d not %d", i, num)
			num++
		}

		// Cause a block to be refreshed to the beginning
		buf := datastore.Get(c, keys[256])
		test.assert(buf != nil, "Block not found")

		data := datastore.lru.Back().Value.(buffer)
		i := int(data.data[1]) + int(data.data[2])*256
		test.assert(i == 256, "Incorrect most recent block %d != 256", i)

		data = datastore.lru.Front().Value.(buffer)
		i = int(data.data[1]) + int(data.data[2])*256
		test.assert(i == 255, "Incorrect least recent block %d != 255", i)
	})
}
