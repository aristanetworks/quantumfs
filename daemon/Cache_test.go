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

func createBuffer(c *quantumfs.Ctx, test *testHelper, backingStore *testDataStore,
	datastore *dataStore, keys map[int]quantumfs.ObjectKey, indx, size int) {
	bytes := make([]byte, size*quantumfs.ObjectKeyLength)
	bytes[1] = byte(indx % 256)
	bytes[2] = byte(indx / 256)
	key := quantumfs.NewObjectKeyFromBytes(
		bytes[:quantumfs.ObjectKeyLength])
	keys[indx] = key
	buff := &buffer{
		data:      bytes,
		dirty:     false,
		keyType:   quantumfs.KeyTypeData,
		key:       key,
		dataStore: datastore,
	}
	err := backingStore.Set(c, key, buff)
	test.Assert(err == nil, "Error priming datastore: %v", err)

}

func fillDatastore(c *quantumfs.Ctx, test *testHelper, backingStore *testDataStore,
	datastore *dataStore, entryNum int, keys map[int]quantumfs.ObjectKey) {

	for i := 1; i < entryNum; i++ {
		_, exists := keys[i]
		// Only fill the keys which haven't been filled
		if !exists {
			createBuffer(c, test, backingStore, datastore, keys, i, 1)
		}
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
		entryNum := 512
		cacheSize := (entryNum / 2) * quantumfs.ObjectKeyLength
		c, backingStore, datastore, keys := createDatastore(test,
			entryNum, cacheSize)
		fillDatastore(c, test, backingStore, datastore, entryNum, keys)

		// Prime the LRU by reading every entry in reverse order. At the end
		// we should have the first (entryNum/2) entries in the cache.
		test.Log("Priming LRU")
		for i := entryNum - 1; i > 0; i-- {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed retrieving block %d", i)
		}
		test.Log("Verifying cache")
		test.Assert(datastore.cacheSize == cacheSize,
			"Incorrect Cache size %d != %d", datastore.cacheSize,
			cacheSize)
		lruNum := cacheSize / quantumfs.ObjectKeyLength
		for _, v := range datastore.cache {
			i := int(v.data[1]) + int(v.data[2])*256
			test.Assert(i <= lruNum,
				"Unexpected block in cache %d", i)
		}
		test.Log("Verifying LRU")
		freeSpace := cacheSize % quantumfs.ObjectKeyLength
		test.Assert(datastore.lru.Len() == lruNum,
			"Incorrect Lru size %d != %d ", lruNum, datastore.lru.Len())
		test.Assert(datastore.freeSpace == freeSpace,
			"Incorrect Free space %d != %d", freeSpace,
			datastore.freeSpace)
		num := 1
		for e := datastore.lru.Back(); e != nil; e = e.Prev() {
			buf := e.Value.(buffer)
			i := int(buf.data[1]) + int(buf.data[2])*256
			test.Assert(i <= lruNum, "Unexpected block in lru %d", i)
			test.Assert(i == num, "Out of order block %d not %d", i, num)
			num++
		}

		// Cause a block to be refreshed to the beginning
		buf := datastore.Get(c, keys[256])
		test.Assert(buf != nil, "Block not found")

		data := datastore.lru.Back().Value.(buffer)
		i := int(data.data[1]) + int(data.data[2])*256
		test.Assert(i == 256, "Incorrect most recent block %d != 256", i)

		data = datastore.lru.Front().Value.(buffer)
		i = int(data.data[1]) + int(data.data[2])*256
		test.Assert(i == 255, "Wrong least recent block %d != 255", i)
	})
}

func TestCacheCaching(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		entryNum := 256
		c, backingStore, datastore, keys := createDatastore(test,
			entryNum, 100*quantumfs.ObjectKeyLength)
		// Add a content with size greater than datastore.cacheSize, and
		// double the size of keys[1] in advance.
		createBuffer(c, test, backingStore, datastore, keys, 257, 101)
		createBuffer(c, test, backingStore, datastore, keys, 1, 2)
		fillDatastore(c, test, backingStore, datastore, entryNum, keys)

		// Prime the cache
		for i := 1; i <= 100; i++ {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed to get block %d", i)
		}
		buf := datastore.Get(c, keys[257])
		test.Assert(buf != nil, "Failed to get block 257")
		test.Assert(buf.Size() == 101*quantumfs.ObjectKeyLength,
			"Incorrect length of block 257: %d != %d", buf.Size(),
			101*quantumfs.ObjectKeyLength)

		// Since the size of keys[1] is doubled, so it is removed from the
		// cache, so the usage of space is 99*quantumfs.ObjectKeyLength
		test.Assert(datastore.freeSpace == quantumfs.ObjectKeyLength,
			"Failed memory management: %d != %d", datastore.freeSpace,
			quantumfs.ObjectKeyLength)

		backingStore.shouldRead = false

		// Because of the size constraint, the least recent used entry
		// keys[1] should be deleted from cache
		_, exists := datastore.cache[keys[1]]
		test.Assert(!exists, "Failed to forget block 1")
		// The content is oversized, so it should be stored in the cache
		_, exists = datastore.cache[keys[257]]
		test.Assert(!exists, "Failed to forget block 257")

		// Reading again should come entirely from the cache. If not
		// testDataStore will assert.
		for i := 2; i <= 100; i++ {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed to get block %d", i)
		}
	})
}

func TestCacheLruDiffSize(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		entryNum := 280
		cacheSize := 12 + 100*quantumfs.ObjectKeyLength
		c, backingStore, datastore, keys := createDatastore(test,
			entryNum, cacheSize)
		// Add a content with size greater than datastore.cacheSize, and set
		// different sizes of several keys in advance.
		createBuffer(c, test, backingStore, datastore, keys, 1, 2)
		createBuffer(c, test, backingStore, datastore, keys, 4, 15)
		createBuffer(c, test, backingStore, datastore, keys, 13, 6)
		createBuffer(c, test, backingStore, datastore, keys, 71, 30)
		createBuffer(c, test, backingStore, datastore, keys, 257, 120)
		fillDatastore(c, test, backingStore, datastore, entryNum, keys)

		test.Log("Priming LRU")
		for i := entryNum - 1; i > 0; i-- {
			buf := datastore.Get(c, keys[i])
			test.Assert(buf != nil, "Failed retrieving block %d", i)
		}
		// Update keys[257] whose size is greater than cacheSize
		buf := datastore.Get(c, keys[257])
		test.Assert(buf != nil, "Failed retrieving block 257")

		test.Log("Verifying cache")
		test.Assert(datastore.cacheSize == cacheSize,
			"Incorrect cache size %d != %d", datastore.cacheSize,
			cacheSize)
		// Since keys[71] is too large, cache will contain the first 70
		// entries, and we can calculate the free space accordingly
		lruNum := 70
		for _, v := range datastore.cache {
			i := int(v.data[1]) + int(v.data[2])*256
			test.Assert(i <= lruNum,
				"Unexpected block in cache %d", i)
		}
		test.Log("Verifying LRU")
		test.Assert(datastore.lru.Len() == lruNum,
			"Incorrect Lru size %d != %d ", lruNum, datastore.lru.Len())
		freeSpace := 10*quantumfs.ObjectKeyLength + 12
		test.Assert(datastore.freeSpace == freeSpace,
			"Incorrect Free space %d != %d", freeSpace,
			datastore.freeSpace)
		num := 1
		for e := datastore.lru.Back(); e != nil; e = e.Prev() {
			buf := e.Value.(buffer)
			i := int(buf.data[1]) + int(buf.data[2])*256
			test.Assert(i <= lruNum, "Unexpected block in lru %d", i)
			test.Assert(i == num, "Out of order block %d not %d", i, num)
			num++
		}
	})
}
