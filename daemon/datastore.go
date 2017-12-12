// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"container/list"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

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

var inLowMemoryMode bool

// If we receive the signal SIGUSR1, then we will prevent further writes to the cache
// and drop the contents of the cache. The intended use is as a way to free the bulk
// of the memory used by quantumfsd when it is being gracefully shutdown by lazily
// unmounting it.
func signalHandler(store *dataStore, sigUsr1Chan chan os.Signal,
	quit chan struct{}) {

	for {
		select {
		case <-sigUsr1Chan:
			func() {
				defer store.cacheLock.Lock().Unlock()

				// Prevent future additions
				store.cacheSize = -1
				store.freeSpace = -1
				store.cache = make(map[string]*buffer, 0)
				store.lru = list.List{}
			}()

			// Release the memory
			debug.FreeOSMemory()
			inLowMemoryMode = true

		case <-quit:
			signal.Stop(sigUsr1Chan)
			close(sigUsr1Chan)
			return
		}
	}
}

func newDataStore(durableStore quantumfs.DataStore, cacheSize int) *dataStore {
	entryNum := cacheSize / 102400
	store := &dataStore{
		durableStore: durableStore,
		cache:        make(map[string]*buffer, entryNum),
		cacheSize:    cacheSize,
		freeSpace:    cacheSize,
		quit:         make(chan struct{}),
	}

	sigUsr1Chan := make(chan os.Signal, 1)
	signal.Notify(sigUsr1Chan, syscall.SIGUSR1)
	go signalHandler(store, sigUsr1Chan, store.quit)

	return store
}

type dataStore struct {
	durableStore quantumfs.DataStore

	cacheLock utils.DeferableMutex
	lru       list.List // Back is most recently used
	cache     map[string]*buffer
	cacheSize int
	freeSpace int
	quit      chan struct{} // Signal termination
}

func (store *dataStore) shutdown() {
	store.quit <- struct{}{}
}

func (store *dataStore) storeInCache(c *quantumfs.Ctx, buf buffer) {
	defer c.FuncIn(qlog.LogDaemon, "dataStore::storeInCache", "Key: %s",
		buf.key.String()).Out()

	size := buf.Size()

	// Store in cache
	defer store.cacheLock.Lock().Unlock()

	if size > store.cacheSize {
		if store.cacheSize != -1 {
			c.Wlog(qlog.LogDaemon, "The size of content is greater than"+
				" total capacity of the cache")
		}
		return
	}

	if _, exists := store.cache[buf.key.String()]; exists {
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

	store.freeSpace -= size
	for store.freeSpace < 0 {
		evictedBuf := store.lru.Remove(store.lru.Front()).(buffer)
		store.freeSpace += evictedBuf.Size()
		delete(store.cache, evictedBuf.key.String())
	}
	store.cache[buf.key.String()] = &buf
	buf.lruElement = store.lru.PushBack(buf)
}

const CacheHitLog = "Found key in readcache"
const CacheMissLog = "Cache miss"

func (store *dataStore) Get(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) quantumfs.Buffer {

	defer c.FuncIn(qlog.LogDaemon, "dataStore::Get",
		"key %s", key.String()).Out()

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to fetch embedded key")
	}

	// Check cache
	bufResult := func() quantumfs.Buffer {
		defer store.cacheLock.Lock().Unlock()

		if buf, exists := store.cache[key.String()]; exists {
			store.lru.MoveToBack(buf.lruElement)
			return buf.clone()
		}
		return nil
	}()
	if bufResult != nil {
		c.Vlog(qlog.LogDaemon, CacheHitLog)
		return bufResult
	}
	c.Vlog(qlog.LogDaemon, CacheMissLog)

	buf := newEmptyBuffer()
	initBuffer(&buf, store, key)

	err := quantumfs.ConstantStore.Get(c, key, &buf)
	if err == nil {
		c.Vlog(qlog.LogDaemon, "Found key in constant store")
		return &buf
	}

	err = store.durableStore.Get(c, key, &buf)
	if err == nil {
		store.storeInCache(c, buf)
		c.Vlog(qlog.LogDaemon, "Found key in durable store")
		// buf might be stored in the cache, give the client a copy to
		// prevent it from screwing up the content in the cache.
		return buf.clone()
	}
	c.Elog(qlog.LogDaemon, "Couldn't get from any store: %s. Key %s",
		err.Error(), key.String())

	return nil
}

func (store *dataStore) Set(c *quantumfs.Ctx, buf quantumfs.Buffer) error {
	defer c.FuncInName(qlog.LogDaemon, "dataStore::Set").Out()

	key, err := buf.Key(c)
	if err != nil {
		c.Vlog(qlog.LogDaemon, "Error computing key %s", err.Error())
		return err
	}

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to set embedded key")
	}
	buf_ := buf.(*buffer)
	store.storeInCache(c, *buf_)
	return store.durableStore.Set(c, key, buf)
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
	inSize := len(in)

	defer c.FuncIn("newBufferCopy", "keyType %d inSize %d", keyType,
		len(in)).Out()

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
		dataStore: c.dataStore,
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
	data       []byte
	dirty      bool
	keyType    quantumfs.KeyType
	key        quantumfs.ObjectKey
	dataStore  *dataStore
	lruElement *list.Element
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

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	defer c.FuncInName(qlog.LogDaemon, "buffer::Key").Out()

	if !buf.dirty {
		c.Vlog(qlog.LogDaemon, "Buffer not dirty")
		return buf.key, nil
	}

	buf.key = quantumfs.NewObjectKey(buf.keyType, buf.ContentHash())
	buf.dirty = false
	c.Vlog(qlog.LogDaemon, "New buffer key %s", buf.key.String())
	err := buf.dataStore.Set(c, buf)
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
