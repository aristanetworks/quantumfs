// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"container/list"
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/encoding"
	"github.com/aristanetworks/quantumfs/hash"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	capn "github.com/glycerine/go-capnproto"
)

var zeros []byte

func init() {
	zeros = make([]byte, quantumfs.MaxBlockSize)
}

func newDataStore(durableStore quantumfs.DataStore, cacheSize int) *dataStore {
	store := &dataStore{
		durableStore: durableStore,
		cache:        newCombiningCache(cacheSize),
	}

	return store
}

type dataStore struct {
	durableStore quantumfs.DataStore
	cache        *combiningCache
}

func (store *dataStore) shutdown() {
	store.cache.shutdown()
}

const CacheHitLog = "Found key in readcache"
const CacheMissLog = "Cache miss"

func (store *dataStore) Freshen(c *ctx, key quantumfs.ObjectKey) error {
	// TODO: Make this function part of the quantumfs datastore interface
	buf := store.Get(&c.Ctx, key)
	if buf == nil {
		return fmt.Errorf("Cannot freshen %s, "+
			"block missing from db", key.String())
	}

	err := store.durableStore.Set(&c.Ctx, key, buf.CastToMutable())
	return err
}

func (store *dataStore) Get(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) *ImmutableBuffer {

	defer c.FuncIn(qlog.LogDaemon, "dataStore::Get",
		"key %s", key.String()).Out()

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to fetch embedded key")
	}

	var thinBuf buffer
	initBuffer(&thinBuf, store, key)
	err := quantumfs.ConstantStore.Get(c, key, &thinBuf)
	if err == nil {
		c.Vlog(qlog.LogDaemon, "Found key in constant store")
		return newImmutableBufferWithKey(thinBuf.data, thinBuf.key,
			thinBuf.dataStore)
	}

	// Check cache
	bufResult, resultChannel := store.cache.get(c, key,
		func() *ImmutableBuffer {
			buf := newEmptyBuffer()
			initBuffer(&buf, store, key)

			err := store.durableStore.Get(c, key, &buf)
			if err != nil {
				c.Elog(qlog.LogDaemon, "Couldn't get from any "+
					"store: %s Key %s", err.Error(),
					key.String())
				return nil
			}

			return newImmutableBufferWithKey(buf.data, buf.key,
				buf.dataStore)
		})

	if bufResult != nil {
		c.Vlog(qlog.LogDaemon, CacheHitLog)
		return bufResult
	}
	c.Vlog(qlog.LogDaemon, CacheMissLog)

	return <-resultChannel
}

func (store *dataStore) Set(c *quantumfs.Ctx, buf *ImmutableBuffer) error {

	defer c.FuncInName(qlog.LogDaemon, "dataStore::Set").Out()

	key := buf.Key()
	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to set embedded key")
	}

	if key.IsEqualTo(quantumfs.ZeroKey) {
		panic("Attempted Set without provided Key")
	}

	store.cache.storeInCache(c, key, buf)
	return store.durableStore.Set(c, key, buf.CastToMutable())
}

func newEmptyBuffer() buffer {
	return buffer{
		data: make([]byte, 0, initBlockSize),
	}
}

// Does not obey the initBlockSize capacity, so only for use with buffers that
// are very unlikely to be written to
func newBuffer(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	defer c.FuncIn("newBuffer", "keyType %d", keyType).Out()

	return &buffer{
		data:      in,
		dirty:     true,
		keyType:   keyType,
		dataStore: c.dataStore,
	}
}

// Like newBuffer(), but 'in' is copied and ownership is not assumed
func newBufferCopy(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	defer c.FuncIn("newBufferCopy", "keyType %d inSize %d", keyType,
		len(in)).Out()

	return newBufferCopyDs(c.dataStore, in, keyType)
}

func newBufferCopyDs(store *dataStore, in []byte,
	keyType quantumfs.KeyType) quantumfs.Buffer {

	inSize := len(in)

	var newData []byte
	// ensure our buffer meets min capacity
	if inSize < initBlockSize {
		newData = make([]byte, inSize, initBlockSize)
	} else {
		newData = make([]byte, inSize)
	}
	copy(newData, in)

	return &buffer{
		data:      newData,
		dirty:     true,
		keyType:   keyType,
		dataStore: store,
	}
}

func initBuffer(buf *buffer, dataStore *dataStore, key quantumfs.ObjectKey) {
	buf.dirty = false
	buf.dataStore = dataStore
	buf.keyType = key.Type()
	buf.key = key
}

// Return a buffer with the same contents, but modified such that there can be no
// modification to the original backing data.
func (buf *buffer) clone() *buffer {
	// For now, just copy the internal data. In theory we can be clever later.

	newBuf := &buffer{
		dirty:     buf.dirty,
		keyType:   buf.keyType,
		key:       buf.key,
		dataStore: buf.dataStore,
		// Don't include in cache
	}

	newData := make([]byte, len(buf.data))
	copy(newData, buf.data)
	newBuf.data = newData

	return newBuf
}

type buffer struct {
	data      []byte
	dirty     bool
	keyType   quantumfs.KeyType
	key       quantumfs.ObjectKey
	dataStore *dataStore
}

func (buf *buffer) padWithZeros(newLength int) {
	buf.data = appendAndExtendCap(buf.data, zeros[:(newLength-len(buf.data))])
}

// this gives us append functionality, while doubling capacity on reallocation
// instead of what append does, which is increase by 25% after 1KB
func appendAndExtendCap(arrA []byte, arrB []byte) []byte {
	dataLen := len(arrA)
	newLen := len(arrA) + len(arrB)
	oldCap := cap(arrA)
	newCap := oldCap

	if newCap == 0 {
		newCap = 1
	}

	for ; newLen > newCap; newCap += newCap {
		// double the capacity until everything fits
	}

	if newCap != oldCap {
		// We need to more than double the capacity in order to trick
		// append into using our capacity instead of just adding 25%
		newCap += newCap
		if newCap > quantumfs.MaxBlockSize {
			newCap = quantumfs.MaxBlockSize
		}

		// We need to fill arrA and then double it to add capacity
		toAppendLen := newCap - dataLen

		// Use the zeros array instead of making our data. Using append and
		// taking a subslice should result in just capacity increasing
		rtn := append(arrA, zeros[:toAppendLen]...)
		copy(rtn[dataLen:], arrB)

		// Take the subslice here so length is correct and cap is larger
		return rtn[:newLen]
	}

	return append(arrA, arrB...)
}

func (buf *buffer) Write(c *quantumfs.Ctx, in []byte, offset_ uint32) uint32 {
	defer c.FuncIn(qlog.LogDaemon, "buffer::Write", "size %d offset %d",
		len(in), offset_).Out()

	offset := int(offset_)
	// Sanity check offset and length
	maxWriteLen := quantumfs.MaxBlockSize - offset
	if maxWriteLen <= 0 {
		c.Vlog(qlog.LogDaemon, "maxWriteLen <= 0")
		return 0
	}

	if len(in) > maxWriteLen {
		in = in[:maxWriteLen]
	}

	if offset > len(buf.data) {
		// Expand a hole of zeros
		buf.padWithZeros(offset)
	}

	// At this point there is data leading up to the offset (and maybe past)
	var copied int
	if offset+len(in) > len(buf.data) {
		// We know we have to increase the buffer size... so append!
		buf.data = appendAndExtendCap(buf.data[:offset], in)
		copied = len(in)
	} else {
		// This is the easy case. No buffer enlargement
		copied = copy(buf.data[offset:], in)
	}

	c.Vlog(qlog.LogDaemon, "Marking buffer dirty")
	buf.dirty = true

	return uint32(copied)
}

func (buf *buffer) Read(out []byte, offset uint32) int {
	return copy(out, buf.data[offset:])
}

func (buf *buffer) Get() []byte {
	return buf.data
}

func (buf *buffer) Set(data []byte, keyType quantumfs.KeyType) {
	// ensure our buffer meets min size
	if cap(data) < initBlockSize {
		newData := make([]byte, len(data), initBlockSize)
		copy(newData, data)
		data = newData
	}

	buf.data = data
	buf.keyType = keyType
	buf.dirty = true
}

func (buf *buffer) KeyType() quantumfs.KeyType {
	return buf.keyType
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	defer c.FuncInName(qlog.LogDaemon, "buffer::Key").Out()

	if !buf.dirty {
		c.Vlog(qlog.LogDaemon, "Buffer not dirty")
		return buf.key, nil
	}

	setCopy := newImmutableBufferDataCopy(buf.data, buf.keyType, buf.dataStore)

	buf.key = setCopy.Key()
	buf.dirty = false
	c.Vlog(qlog.LogDaemon, "New buffer key %s", buf.key.String())
	err := buf.dataStore.Set(c, setCopy)
	return buf.key, err
}

func (buf *buffer) SetSize(size int) {
	switch {
	case size > quantumfs.MaxBlockSize:
		panic(fmt.Sprintf("New block size greater than maximum: %d",
			size))
	case size > len(buf.data):
		// We have to increase our capacity first
		buf.padWithZeros(size)
	case size == len(buf.data):
		// No change
		return
	case size < len(buf.data):
		buf.data = buf.data[:size]
	}

	buf.dirty = true
}

func (buf *buffer) Size() int {
	return len(buf.data)
}

func (buf *buffer) AsDirectoryEntry() quantumfs.DirectoryEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayDirectoryEntry(
		encoding.ReadRootDirectoryEntry(segment))

}

func (buf *buffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayWorkspaceRoot(
		encoding.ReadRootWorkspaceRoot(segment))

}

func (buf *buffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayMultiBlockFile(
		encoding.ReadRootMultiBlockFile(segment))

}

func (buf *buffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayVeryLargeFile(
		encoding.ReadRootVeryLargeFile(segment))

}

func (buf *buffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))

}

func (buf *buffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}

type cacheEntry struct {
	buf        *ImmutableBuffer
	waiting    []chan *ImmutableBuffer
	lruElement *list.Element
}

type combiningCache struct {
	lock utils.DeferableMutex

	lru      list.List // Back is most recently used
	entryMap map[string]*cacheEntry

	size      int
	freeSpace int
}

func newCombiningCache(cacheSize int) *combiningCache {
	entryNum := cacheSize / 102400

	return &combiningCache{
		entryMap:  make(map[string]*cacheEntry, entryNum),
		size:      cacheSize,
		freeSpace: cacheSize,
	}
}

func (cc *combiningCache) shutdown() {
	defer cc.lock.Lock().Unlock()

	// Prevent future additions
	cc.size = -1
	cc.freeSpace = -1
	cc.entryMap = make(map[string]*cacheEntry, 0)
	cc.lru = list.List{}
}

// get either returns a buffer copy from the cache, or a channel that the buffer
// will come back on
func (cc *combiningCache) get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	fetch func() *ImmutableBuffer) (cached *ImmutableBuffer,
	resultChannel chan *ImmutableBuffer) {

	defer cc.lock.Lock().Unlock()

	entry, exists := cc.entryMap[key.String()]
	if exists {
		if entry.buf != nil {
			// refresh an existing cached entry
			cc.lru.MoveToBack(entry.lruElement)
			return entry.buf, nil
		} // else we need to wait for the result
	} else {
		// Prepare a placeholder to indicate the result is being fetched
		entry = &cacheEntry{
			buf:     nil,
			waiting: make([]chan *ImmutableBuffer, 0),
		}

		// Asynchronously fetch and store the result
		go func() {
			// Note: to prevent deadlock when testing we want to fetch
			// in the goroutine
			cc.storeInCache(c, key, fetch())
		}()
	}

	// Waiting for data, so add on a channel
	waitChan := make(chan *ImmutableBuffer)
	entry.waiting = append(entry.waiting, waitChan)
	// Note: the cache entry will only get pushed into the lru queue when its
	// data is set into the cache
	cc.entryMap[key.String()] = entry

	return nil, waitChan
}

func (cc *combiningCache) storeInCache(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf *ImmutableBuffer) {

	defer c.FuncIn(qlog.LogDaemon, "dataStore::storeInCache", "Key: %s",
		key.String()).Out()

	defer cc.lock.Lock().Unlock()

	// Satisfy any channels waiting for this data, no matter what
	entry, exists := cc.entryMap[key.String()]
	if exists && entry.buf == nil && entry.waiting != nil {
		// Send copies to each waiter and then nullify the queue of them
		for _, channel := range entry.waiting {
			channel <- buf
		}
		entry.waiting = nil
	}

	// Empty buffer, nothing to do but clear the entry
	if buf == nil {
		delete(cc.entryMap, key.String())
		return
	}

	// Now determine whether we can actually store in the cache
	size := buf.Size()
	if size > cc.size {
		if cc.size != -1 {
			c.Wlog(qlog.LogDaemon, "The size of content is greater than"+
				" total capacity of the cache")
		}
		// ensure we remove any entries that may signal that we're waiting
		// for this data to come back any more
		delete(cc.entryMap, buf.Key().String())
		return
	}

	if exists && entry.buf != nil {
		// It is possible when storing dirty data that we could reproduce the
		// contents which already exist in the cache. We don't want to have
		// the same data in the LRU queue twice as that is wasteful, though
		// eventually the 'overwritten' buffer will be evicted and the space
		// recovered.
		//
		// Now if we have reproduced the same data already in the cache we
		// could move/set the buffer to be the most recently used. We choose
		// not to do that because inserting newly uploaded data into the
		// cache is an optimization for the relatively common case where a
		// file is written and then read shortly afterwards. However, that
		// doesn't always happen. Instead we will leave the data's LRU
		// position unchanged. Should the data be reread in short order then
		// it is likely to still be in the cache. However, if the data isn't
		// read in short order then marking that data most recently used will
		// simply force something else out of the cache.
		c.Vlog(qlog.LogDaemon, "Not touching key in cache")
		return
	}

	// Place it in the cache
	cc.freeSpace -= size
	for cc.freeSpace < 0 {
		// Note: cache entries that are still awaiting their data are not
		// placed in the lru yet and as such will never be evicted
		evictedBuf := cc.lru.Remove(cc.lru.Front()).(*cacheEntry)
		cc.freeSpace += evictedBuf.buf.Size()
		delete(cc.entryMap, evictedBuf.buf.Key().String())
	}

	newEntry := &cacheEntry{
		buf: buf,
	}
	newEntry.lruElement = cc.lru.PushBack(newEntry)
	cc.entryMap[buf.Key().String()] = newEntry
}

type ImmutableBuffer struct {
	data      []byte
	key       quantumfs.ObjectKey
	dataStore *dataStore
}

func newImmutableBufferDataCopy(in []byte, keyType_ quantumfs.KeyType,
	store *dataStore) *ImmutableBuffer {

	inSize := len(in)

	var newData []byte
	// ensure our buffer meets min capacity
	if inSize < initBlockSize {
		newData = make([]byte, inSize, initBlockSize)
	} else {
		newData = make([]byte, inSize)
	}
	copy(newData, in)

	return newImmutableBuffer(newData, keyType_, store)
}

func newImmutableBuffer(newData []byte, keyType quantumfs.KeyType,
	store *dataStore) *ImmutableBuffer {

	return &ImmutableBuffer{
		data:      newData,
		key:       quantumfs.NewObjectKey(keyType, hash.Hash(newData)),
		dataStore: store,
	}
}

func newImmutableBufferWithKey(newData []byte, key quantumfs.ObjectKey,
	store *dataStore) *ImmutableBuffer {

	return &ImmutableBuffer{
		data:      newData,
		key:       key,
		dataStore: store,
	}
}

func (buf *ImmutableBuffer) clone() quantumfs.Buffer {
	return newBufferCopyDs(buf.dataStore, buf.data, buf.key.Type())
}

func (buf *ImmutableBuffer) Read(out []byte, offset uint32) int {
	return copy(out, buf.data[offset:])
}

func (buf *ImmutableBuffer) KeyType() quantumfs.KeyType {
	return buf.key.Type()
}

func (buf *ImmutableBuffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *ImmutableBuffer) Key() quantumfs.ObjectKey {
	return buf.key
}

func (buf *ImmutableBuffer) Size() int {
	return len(buf.data)
}

// Should be used very carefully - the returned slice must not be modified
func (buf *ImmutableBuffer) Get() []byte {
	return buf.data
}

func (buf *ImmutableBuffer) CastToMutable() *buffer {
	var rtn buffer
	initBuffer(&rtn, buf.dataStore, buf.key)
	rtn.Set(buf.data, buf.key.Type())
	return &rtn
}
