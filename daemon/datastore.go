// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "container/list"
import "crypto/sha1"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/qlog"
import capn "github.com/glycerine/go-capnproto"

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

	buf := newEmptyBuffer()
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

func newEmptyBuffer() buffer {
	return buffer{
		data: make([]byte, quantumfs.MaxBlockSize),
		size: 0,
	}
}

// buffer is the central data-handling type of quantumfsd. Copying once here into
// a buffer the max size we'll ever need is better than using append a bunch
func newBuffer(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	inSize := len(in)

	// ensure our buffer is of max size
	if inSize < quantumfs.MaxBlockSize {
		newData := make([]byte, quantumfs.MaxBlockSize)
		copy(newData, in)
		in = newData
	}

	return &buffer{
		data:      in,
		size:      inSize,
		dirty:     true,
		keyType:   keyType,
		dataStore: c.dataStore,
	}
}

// Like newBuffer(), but 'in' is copied and ownership is not assumed
func newBufferCopy(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	newData := make([]byte, quantumfs.MaxBlockSize)
	copy(newData, in)
	return &buffer{
		data:      newData,
		size:      len(in),
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
	// changing slice size is expensive. Allocate less, remember the real size
	// and allocate a fixed amount only once
	data []byte
	size int

	dirty      bool
	keyType    quantumfs.KeyType
	key        quantumfs.ObjectKey
	dataStore  *dataStore
	lruElement *list.Element
}

func (buf *buffer) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
	// Sanity check offset and length
	maxWriteLen := quantumfs.MaxBlockSize - int(offset)
	if maxWriteLen <= 0 {
		return 0
	}

	if len(in) > maxWriteLen {
		in = in[:maxWriteLen]
	}

	// Ensure that our data ends where we need it to. Clear with zeros.
	for i := buf.size; i < int(offset); i++ {
		buf.data[i] = 0
	}

	// copy the data right in there. This is great 'cause it preserves data
	// that already lies past where we're writing
	copied := uint32(copy(buf.data[offset:], in))

	// update size, compensating for overwrites
	copyPastEndIdx := offset + copied
	if copyPastEndIdx > uint32(buf.size) {
		buf.size = int(copyPastEndIdx)
	}

	c.Vlog(qlog.LogDaemon, "Marking buffer dirty")
	buf.dirty = true

	return uint32(copied)
}

func (buf *buffer) Read(out []byte, offset uint32) int {
	return copy(out, buf.data[offset:buf.size])
}

func (buf *buffer) Get() []byte {
	return buf.get_()
}

func (buf *buffer) get_() []byte {
	return buf.data[:buf.size]
}

func (buf *buffer) Set(data []byte, keyType quantumfs.KeyType) {
	copy(buf.data, data)
	buf.size = len(data)
	buf.keyType = keyType
	buf.dirty = true
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return sha1.Sum(buf.get_())
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

	if buf.size > size {
		buf.size = size
		return
	}

	for i := buf.size; i < size; i++ {
		buf.data[i] = 0
	}
	buf.size = size

	buf.dirty = true
}

func (buf *buffer) Size() int {
	return buf.size
}

func (buf *buffer) AsDirectoryEntry() quantumfs.DirectoryEntry {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayDirectoryEntry(
		encoding.ReadRootDirectoryEntry(segment))

}

func (buf *buffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayWorkspaceRoot(
		encoding.ReadRootWorkspaceRoot(segment))

}

func (buf *buffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayMultiBlockFile(
		encoding.ReadRootMultiBlockFile(segment))

}

func (buf *buffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayVeryLargeFile(
		encoding.ReadRootVeryLargeFile(segment))

}

func (buf *buffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))

}

func (buf *buffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.get_())
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
