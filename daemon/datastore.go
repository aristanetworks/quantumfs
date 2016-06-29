// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "crypto/sha1"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

func newDataStore(durableStore quantumfs.DataStore) *dataStore {
	return &dataStore{
		durableStore: durableStore,
	}
}

type dataStore struct {
	durableStore quantumfs.DataStore
}

func (store *dataStore) Get(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) quantumfs.Buffer {

	var buf buffer
	initBuffer(&buf, store, key)

	err := quantumfs.ConstantStore.Get(key, &buf)
	if err == nil {
		return &buf
	}

	err = store.durableStore.Get(key, &buf)
	if err == nil {
		return &buf
	}
	c.Elog(qlog.LogDaemon, "Couldn't get from any store: %v. Key %s", err, key)

	return nil
}

func (store *dataStore) Set(c *quantumfs.Ctx, buffer quantumfs.Buffer) error {
	key, err := buffer.Key(c)
	if err != nil {
		return err
	}
	return store.durableStore.Set(key, buffer)
}

// buffer is the central data-handling type of quantumfsd
func newBuffer(c *ctx, in []byte, keyType quantumfs.KeyType) quantumfs.Buffer {
	return &buffer{
		data:      in,
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
	data      []byte
	dirty     bool
	keyType   quantumfs.KeyType
	key       quantumfs.ObjectKey
	dataStore *dataStore
}

func (buf *buffer) Write(in []byte, offset uint32) uint32 {
	// Sanity check offset and length
	maxWriteLen := quantumfs.MaxBlockSize - int(offset)
	if maxWriteLen <= 0 {
		return 0
	}

	if len(in) > maxWriteLen {
		in = in[:maxWriteLen]
	}

	// Ensure that our data ends where we need it to. This allows us to write
	// past the end of a block, but not past the block's max capacity
	deltaLen := int(offset) - len(buf.data)
	if deltaLen > 0 {
		buf.data = append(buf.data, make([]byte, deltaLen)...)
	}

	var finalBuffer []byte
	// append our write data to the first split of the existing data
	finalBuffer = append(buf.data[:offset], in...)

	// record how much was actually appended (in case len(in) < size)
	copied := uint32(len(finalBuffer)) - uint32(offset)

	// then add on the rest of the existing data afterwards, excluding the amount
	// that we just wrote (to overwrite instead of insert)
	remainingStart := offset + copied
	if int(remainingStart) < len(buf.data) {
		finalBuffer = append(finalBuffer, buf.data[remainingStart:]...)
	}

	buf.dirty = true
	buf.data = finalBuffer

	return copied
}

func (buf *buffer) Get() []byte {
	return buf.data
}

func (buf *buffer) Set(data []byte, keyType quantumfs.KeyType) {
	buf.data = data
	buf.keyType = keyType
	buf.dirty = true
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return sha1.Sum(buf.data)
}

func (buf *buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	if !buf.dirty {
		return buf.key, nil
	}

	buf.key = quantumfs.NewObjectKey(buf.keyType, buf.ContentHash())
	buf.dirty = false
	err := buf.dataStore.Set(c, buf)
	return buf.key, err
}
