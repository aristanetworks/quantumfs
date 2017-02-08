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
		data: make([]byte, quantumfs.InitBlockSize),
		size: 0,
	}
}

// buffer is the central data-handling type of quantumfsd. Copying once here into
// a buffer the max size we'll ever need is better than using append a bunch
func newBuffer(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	inSize := len(in)

	// ensure our buffer meets min size
	if inSize < quantumfs.InitBlockSize {
		newData := make([]byte, quantumfs.InitBlockSize)
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
	inSize := len(in)

	var newData []byte
	// ensure our buffer meets min size
	if inSize < quantumfs.InitBlockSize {
		newData = make([]byte, quantumfs.InitBlockSize)
	} else {
		newData = make([]byte, inSize)
	}
	copy(newData, in)

	return &buffer{
		data:      newData,
		size:      inSize,
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

func calcNewCap(oldSize int, proposedLen int) int {
	newCap := oldSize
	for {
		newCap *= 2
		if newCap >= proposedLen {
			return newCap
		}
	}
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

	// we want to minimize append / make. Since append has to make and copy,
	// we can't get any better than a single make and two copies
	pastEndIdx := offset + uint32(len(in))
	if pastEndIdx > uint32(len(buf.data)) {
		// We can take advantage of the fact that new memory is zero'd,
		// but can't avoid two copies and a make

		newCap := calcNewCap(len(buf.data), int(pastEndIdx))
		newData := make([]byte, newCap)
		copy(newData, buf.data)
		buf.data = newData
	} else {
		// This is the easy case. No buffer enlargement

		// Ensure that our data ends where we need it to. Clear with zeros.
		for i := buf.size; i < int(offset); i++ {
			buf.data[i] = 0
		}
	}

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
	return buf.get()
}

func (buf *buffer) get() []byte {
	return buf.data[:buf.size]
}

func (buf *buffer) Set(data []byte, keyType quantumfs.KeyType) {
	inSize := len(data)

	// ensure our buffer meets min size
	if inSize < quantumfs.InitBlockSize {
		newData := make([]byte, quantumfs.InitBlockSize)
		copy(newData, data)
		data = newData
	}

	buf.data = data
	buf.size = inSize
	buf.keyType = keyType
	buf.dirty = true
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return sha1.Sum(buf.get())
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

	if size > len(buf.data) {
		// we have to increase our capacity first
		newCap := calcNewCap(len(buf.data), size)
		newData := make([]byte, newCap)
		copy(newData, buf.data)
		buf.data = newData
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
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayDirectoryEntry(
		encoding.ReadRootDirectoryEntry(segment))

}

func (buf *buffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayWorkspaceRoot(
		encoding.ReadRootWorkspaceRoot(segment))

}

func (buf *buffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayMultiBlockFile(
		encoding.ReadRootMultiBlockFile(segment))

}

func (buf *buffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayVeryLargeFile(
		encoding.ReadRootVeryLargeFile(segment))

}

func (buf *buffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))

}

func (buf *buffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.get())
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
