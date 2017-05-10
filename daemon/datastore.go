// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "container/list"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/utils"
import capn "github.com/glycerine/go-capnproto"

var zeros []byte

func init() {
	zeros = make([]byte, quantumfs.MaxBlockSize)
}

func newDataStore(durableStore quantumfs.DataStore, cacheSize int) *dataStore {
	entryNum := cacheSize / 102400
	return &dataStore{
		durableStore: durableStore,
		cache:        make(map[quantumfs.ObjectKey]*buffer, entryNum),
		cacheSize:    cacheSize,
		freeSpace:    cacheSize,
	}
}

type dataStore struct {
	durableStore quantumfs.DataStore

	cacheLock utils.DeferableMutex
	lru       list.List // Back is most recently used
	cache     map[quantumfs.ObjectKey]*buffer
	cacheSize int
	freeSpace int
}

func (store *dataStore) Get(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) quantumfs.Buffer {

	defer c.FuncIn(qlog.LogDaemon, "dataStore::Get",
		"key %s", key.Text()).Out()

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to fetch embedded key")
	}

	// Check cache
	bufResult := func() quantumfs.Buffer {
		defer store.cacheLock.Lock().Unlock()

		if buf, exists := store.cache[key]; exists {
			store.lru.MoveToBack(buf.lruElement)
			return buf.clone()
		}
		return nil
	}()
	if bufResult != nil {
		c.Vlog(qlog.LogDaemon, "Found key in readcache")
		return bufResult
	}

	buf := newEmptyBuffer()
	initBuffer(&buf, store, key)

	err := quantumfs.ConstantStore.Get(c, key, &buf)
	if err == nil {
		c.Vlog(qlog.LogDaemon, "Found key in constant store")
		return &buf
	}

	err = store.durableStore.Get(c, key, &buf)
	if err == nil {
		size := buf.Size()

		// Store in cache
		defer store.cacheLock.Lock().Unlock()

		if size > store.cacheSize {
			c.Vlog(qlog.LogDaemon, "The size of content is greater than"+
				" total capacity of the cache")
			return &buf
		}

		store.freeSpace -= size
		for store.freeSpace < 0 {
			evictedBuf := store.lru.Remove(store.lru.Front()).(buffer)
			store.freeSpace += evictedBuf.Size()
			delete(store.cache, evictedBuf.key)
		}
		store.cache[buf.key] = &buf
		buf.lruElement = store.lru.PushBack(buf)

		c.Vlog(qlog.LogDaemon, "Found key in durable store store")
		return &buf
	}
	c.Elog(qlog.LogDaemon, "Couldn't get from any store: %s. Key %s",
		err.Error(), key.Text())

	return nil
}

func (store *dataStore) Set(c *quantumfs.Ctx, buffer quantumfs.Buffer) error {
	defer c.FuncInName(qlog.LogDaemon, "dataStore::Set").Out()

	key, err := buffer.Key(c)
	if err != nil {
		c.Vlog(qlog.LogDaemon, "Error computing key %s", err.Error())
		return err
	}

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to set embedded key")
	}
	return store.durableStore.Set(c, key, buffer)
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
	c.Vlog(qlog.LogDaemon, "New buffer key %s", buf.key.Text())
	err := buf.dataStore.Set(c, buf)
	return buf.key, err
}

func (buf *buffer) SetSize(size int) {
	if size > quantumfs.MaxBlockSize {
		panic("New block size greater than maximum")
	}

	if len(buf.data) > size {
		buf.data = buf.data[:size]
		return
	}

	if size > len(buf.data) {
		// we have to increase our capacity first
		buf.padWithZeros(size)
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
