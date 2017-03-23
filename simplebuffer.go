// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

// This file contains a buffer for the testing purpose only. Some interface functions
// are only briefly implemented with a dummy return value

import "github.com/aristanetworks/quantumfs/encoding"
import capn "github.com/glycerine/go-capnproto"

// SimpleBuffer only contains data and key to meet the requirements of Set() and
// Get() in datastore. This can be used in tests and tools which need to
// use datastore API and thus need an implementation of quantumfs.Buffer
// interface
type SimpleBuffer struct {
	key  ObjectKey
	data []byte
}

func NewSimpleBuffer(in []byte, q_key ObjectKey) Buffer {
	return &SimpleBuffer{
		key:  q_key,
		data: in,
	}
}

// Implement the required interface functions. Only  Get() and Set() will be called,
// so the others will be briefly implemented or be directly copied from
// daemon/datastore.go
func (buf *SimpleBuffer) Write(c *Ctx, in []byte, offset uint32) uint32 {
	panic("Error: The Write function of SimpleBuffer is not implemented")
}

func (buf *SimpleBuffer) Read(out []byte, offset uint32) int {
	panic("Error: The Read function of SimpleBuffer is not implemented")
}

func (buf *SimpleBuffer) Get() []byte {
	return buf.data
}

func (buf *SimpleBuffer) Set(data []byte, keyType KeyType) {
	buf.data = data
}

func (buf *SimpleBuffer) ContentHash() [ObjectKeyLength - 1]byte {
	return Hash(buf.data)
}

func (buf *SimpleBuffer) Key(c *Ctx) (ObjectKey, error) {
	return buf.key, nil
}

func (buf *SimpleBuffer) SetSize(size int) {
	buf.data = buf.data[:size]
}

func (buf *SimpleBuffer) Size() int {
	return len(buf.data)
}

func (buf *SimpleBuffer) AsDirectoryEntry() DirectoryEntry {
	segment := capn.NewBuffer(buf.data)
	return OverlayDirectoryEntry(encoding.ReadRootDirectoryEntry(segment))
}

func (buf *SimpleBuffer) AsWorkspaceRoot() WorkspaceRoot {
	segment := capn.NewBuffer(buf.data)
	return OverlayWorkspaceRoot(encoding.ReadRootWorkspaceRoot(segment))
}

func (buf *SimpleBuffer) AsMultiBlockFile() MultiBlockFile {
	segment := capn.NewBuffer(buf.data)
	return OverlayMultiBlockFile(encoding.ReadRootMultiBlockFile(segment))
}

func (buf *SimpleBuffer) AsVeryLargeFile() VeryLargeFile {
	segment := capn.NewBuffer(buf.data)
	return OverlayVeryLargeFile(encoding.ReadRootVeryLargeFile(segment))
}

func (buf *SimpleBuffer) AsExtendedAttributes() ExtendedAttributes {
	segment := capn.NewBuffer(buf.data)
	return OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))
}

func (buf *SimpleBuffer) AsHardlinkEntry() HardlinkEntry {
	segment := capn.NewBuffer(buf.data)
	return OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
