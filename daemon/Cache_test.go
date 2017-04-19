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

	store.test.Assert(store.shouldRead, "Received unexpected Get for %s",
		key.String())
	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.datastore.Set(c, key, buf)
}

func fillDatastore(c *quantumfs.Ctx, test *testHelper, backingStore *testDataStore,
	datastore *dataStore, entryNum int, keys map[int]quantumfs.ObjectKey) {

	for i := 1; i < 2*entryNum; i++ {
		bytes := make([]byte, quantumfs.ObjectKeyLength)
		bytes[1] = byte(i % 256)
		bytes[2] = byte(i / 256)
		key := quantumfs.NewObjectKeyFromBytes(bytes)
		keys[i] = key
		if i == 1 || i == entryNum {
			bytes = make([]byte, 2*len(bytes))
			bytes[1] = byte(i % 256)
			bytes[2] = byte(i / 256)
		}
		buf := &buffer{
			data:      bytes,
			dirty:     false,
			keyType:   quantumfs.KeyTypeData,
			key:       key,
			dataStore: datastore,
		}
		err := backingStore.Set(c, key, buf)
		test.Assert(err == nil, "Error priming datastore: %v", err)
	}
}

func createDatastore(test *testHelper, entryNum, cacheSize int) (c *quantumfs.Ctx,
	backingStore *testDataStore, datastore *dataStore,
	keys map[int]quantumfs.ObjectKey) {

	backingStore = newTestDataStore(test)
	datastore = newDataStore(backingStore, cacheSize)

	keys = make(map[int]quantumfs.ObjectKey, entryNum)

	ctx := ctx{
		Ctx: quantumfs.Ctx{
			Qlog:      test.Logger,
			RequestId: qlog.TestReqId,
		},
	}
	c = &ctx.Ctx

	return c, backingStore, datastore, keys
}

func TestCacheLru(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		cacheSize := 256
		c, backingStore, datastore, keys := createDatastore(test,
			cacheSize, cacheSize)
		fillDatastore(c, test, backingStore, datastore, cacheSize, keys)

		// Prime the LRU by reading every entry in reverse order. At the end
		// we should have the first cacheSize elements in the cache.
		test.Log("Priming LRU")
		for i := 2*cacheSize - 1; i > 0; i-- {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed retrieving block %d", i)
		}
		test.Log("Verifying cache")
		test.Assert(datastore.cacheSize == cacheSize,
			"Cache size incorrect %d != %d", cacheSize,
			datastore.cacheSize)
		lruSize := cacheSize/quantumfs.ObjectKeyLength - 1
		for _, v := range datastore.cache {
			i := int(v.data[1]) + int(v.data[2])*256
			test.Assert(i <= lruSize,
				"Unexpected block in cache %d", i)
		}
		test.Log("Verifying LRU")
		freeSpace := cacheSize % quantumfs.ObjectKeyLength
		test.Assert(datastore.lru.Len() == lruSize && datastore.freeSpace ==
			freeSpace, "Lru size incorrect %d != %d free space %d != %d",
			lruSize, datastore.lru.Len(), freeSpace, datastore.freeSpace)
		num := 1
		for e := datastore.lru.Back(); e != nil; e = e.Prev() {
			buf := e.Value.(buffer)
			i := int(buf.data[1]) + int(buf.data[2])*256
			test.Assert(i <= lruSize,
				"Unexpected block in lru %d", i)
			test.Assert(i == num, "Out of order block %d not %d", i, num)
			num++
		}

		// Cause a block to be refreshed to the beginning
		buf := datastore.Get(c, keys[256])
		test.Assert(buf != nil, "Block not found")

		data := datastore.lru.Back().Value.(buffer)
		i := int(data.data[1]) + int(data.data[2])*256
		test.Assert(i == 256, "Wrong most recent block %d != 256", i)

		data = datastore.lru.Front().Value.(buffer)
		i = int(data.data[1]) + int(data.data[2])*256
		test.Assert(i == lruSize-2, "Wrong least recent block %d != 255", i)
	})
}

func TestCacheCaching(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		entrySize := 256
		c, backingStore, datastore, keys := createDatastore(test,
			entrySize, 100*quantumfs.ObjectKeyLength)
		fillDatastore(c, test, backingStore, datastore, entrySize, keys)

		// Prime the cache
		for i := 1; i <= 100; i++ {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed to get block %d", i)
		}

		test.Assert(datastore.freeSpace == quantumfs.ObjectKeyLength,
			"Failed memory management: %d != %d", datastore.freeSpace,
			quantumfs.ObjectKeyLength)

		backingStore.shouldRead = false

		// Reading again should come entirely from the cache. If not
		// testDataStore will assert.
		_, exists := datastore.cache[keys[1]]
		test.Assert(!exists, "Failed to forget block 1")
		for i := 2; i <= 100; i++ {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed to get block %d", i)
		}
	})
}
