// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "container/list"
import "crypto/sha1"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/qlog"
import capn "github.com/glycerine/go-capnproto"

var zeros []byte

func init() {
	zeros = make([]byte, quantumfs.MaxBlockSize*4)
}

func newDataStore(durableStore quantumfs.DataStore, cacheSize int) *dataStore {
	return &dataStore{
		durableStore: durableStore,
		cache:        make(map[quantumfs.ObjectKey]*buffer, cacheSize),
		cacheSize:    cacheSize,
	}
}

type dataStore struct {
	durableStore quantumfs.DataStore

	cacheLock DeferableMutex
	lru       list.List // Back is most recently used
	cache     map[quantumfs.ObjectKey]*buffer
	cacheSize int
}

func (store *dataStore) Get(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) quantumfs.Buffer {

	return store.GetSized(c, key, quantumfs.InitBlockSize)
}

func (store *dataStore) GetSized(c *quantumfs.Ctx,
	key quantumfs.ObjectKey, sizeHint int) quantumfs.Buffer {

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to fetch embedded key")
	}

	// Check cache
	bufResult := func() quantumfs.Buffer {
		defer store.cacheLock.Lock().Unlock()

		if buf, exists := store.cache[key]; exists {
			store.lru.MoveToBack(buf.lruElement)
			return buf
		}
		return nil
	}()
	if bufResult != nil {
		return bufResult
	}

	buf := newEmptyBuffer(sizeHint)
	initBuffer(&buf, store, key)

	err := quantumfs.ConstantStore.Get(c, key, &buf)
	if err == nil {
		return &buf
	}

	err = store.durableStore.Get(c, key, &buf)
	if err == nil {
		// Store in cache
		defer store.cacheLock.Lock().Unlock()

		if store.lru.Len() >= store.cacheSize {
			evictedBuf := store.lru.Remove(store.lru.Front())
			delete(store.cache, evictedBuf.(buffer).key)
		}
		store.cache[buf.key] = &buf
		buf.lruElement = store.lru.PushBack(buf)

		return &buf
	}
	c.Elog(qlog.LogDaemon, "Couldn't get from any store: %s. Key %s",
		err.Error(), key.String())

	return nil
}

func (store *dataStore) Set(c *quantumfs.Ctx, buffer quantumfs.Buffer) error {
	key, err := buffer.Key(c)
	if err != nil {
		return err
	}

	if key.Type() == quantumfs.KeyTypeEmbedded {
		panic("Attempted to set embedded key")
	}
	return store.durableStore.Set(c, key, buffer)
}

func newEmptyBuffer(sizeHint int) buffer {
	return buffer{
		data: make([]byte, 0, sizeHint),
	}
}

func newBuffer(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	// ensure our buffer meets min capacity
	if cap(in) < quantumfs.InitBlockSize {
		newData := make([]byte, len(in), quantumfs.InitBlockSize)
		copy(newData, in)
		in = newData
	}

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

	var newData []byte
	// ensure our buffer meets min capacity
	if inSize < quantumfs.InitBlockSize {
		newData = make([]byte, inSize, quantumfs.InitBlockSize)
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

type buffer struct {
	data       []byte
	dirty      bool
	keyType    quantumfs.KeyType
	key        quantumfs.ObjectKey
	dataStore  *dataStore
	lruElement *list.Element
}

// this gives us append functionality, while doubling capacity on reallocation
// instead of what append does, which is increase by 25% after 1KB
func appendDbl(arrA []byte, arrB []byte) []byte {
	dataLen := len(arrA)
	oldCap := cap(arrA)
	newCap := oldCap
	for ; len(arrA) + len(arrB) > newCap; newCap += newCap {
		// double the capacity until everything fits
	}

	if newCap != oldCap {
		// We need to more than double the capacity in order to trick
		// append into using our capacity instead of just adding 25%
		newCap += newCap

		// we need to fill arrA and then double it to add capacity
		toAppendLen := newCap - dataLen

		// use the zeros array instead of making our data. Using append and
		// taking a subslice should result in just capacity increasing
		rtn := append(arrA, zeros[:toAppendLen]...)[:dataLen]

		return append(rtn, arrB...)
	}

	return append(arrA, arrB...)
}

func (buf *buffer) Write(c *quantumfs.Ctx, in []byte, offset_ uint32) uint32 {
	offset := int(offset_)
	// Sanity check offset and length
	maxWriteLen := quantumfs.MaxBlockSize - offset
	if maxWriteLen <= 0 {
		return 0
	}

	if len(in) > maxWriteLen {
		in = in[:maxWriteLen]
	}

	if offset > len(buf.data) {
		// expand a hole of zeros
		buf.data = appendDbl(buf.data, zeros[:(offset - len(buf.data))])
	}

	// at this point there is data leading up to the offset (and maybe past)
	var copied int
	if offset + len(in) > len(buf.data) {
		// we know we have to increase the buffer size... so append!
		buf.data = appendDbl(buf.data[:offset], in)
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
	if cap(data) < quantumfs.InitBlockSize {
		newData := make([]byte, len(data), quantumfs.InitBlockSize)
		copy(newData, data)
		data = newData
	}

	buf.data = data
	buf.keyType = keyType
	buf.dirty = true
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return sha1.Sum(buf.data)
}

func (buf *buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
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
	if size > quantumfs.MaxBlockSize {
		panic("New block size greater than maximum")
	}

	if len(buf.data) > size {
		buf.data = buf.data[:size]
		return
	}

	if size > len(buf.data) {
		// we have to increase our capacity first
		buf.data = appendDbl(buf.data, zeros[:(size - len(buf.data))])
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
