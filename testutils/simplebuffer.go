// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package testutils

// This file contains a buffer for the testing purpose only. Some interface functions
// are only briefly implemented with a dummy return value

import "fmt"
import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/hash"
import capn "github.com/glycerine/go-capnproto"

// SimpleBuffer only contains data and key to meet the requirements of Set() and
// Get() in datastore. This can be used in tests and tools which need to
// use datastore API and thus need an implementation of quantumfs.Buffer
// interface
type SimpleBuffer struct {
	key  quantumfs.ObjectKey
	data []byte
}

func NewSimpleBuffer(in []byte, q_key quantumfs.ObjectKey) quantumfs.Buffer {
	return &SimpleBuffer{
		key:  q_key,
		data: in,
	}
}

func AssertNonZeroBuf(buf quantumfs.Buffer,
	format string, args ...string) {

	if buf.Size() == 0 {
		panic(fmt.Sprintf(format, args))
	}
}

// Implement the required interface functions. Only  Get() and Set() will be called,
// so the others will be briefly implemented or be directly copied from
// daemon/datastore.go
func (buf *SimpleBuffer) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
	panic("Error: The Write function of SimpleBuffer is not implemented")
}

func (buf *SimpleBuffer) Read(out []byte, offset uint32) int {
	panic("Error: The Read function of SimpleBuffer is not implemented")
}

func (buf *SimpleBuffer) Get() []byte {
	return buf.data
}

func (buf *SimpleBuffer) Set(data []byte, keyType quantumfs.KeyType) {
	buf.data = data
}

func (buf *SimpleBuffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *SimpleBuffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	return buf.key, nil
}

func (buf *SimpleBuffer) SetSize(size int) {
	buf.data = buf.data[:size]
}

func (buf *SimpleBuffer) Size() int {
	return len(buf.data)
}

func (buf *SimpleBuffer) AsDirectoryEntry() quantumfs.DirectoryEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayDirectoryEntry(
		encoding.ReadRootDirectoryEntry(segment))
}

func (buf *SimpleBuffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayWorkspaceRoot(
		encoding.ReadRootWorkspaceRoot(segment))
}

func (buf *SimpleBuffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayMultiBlockFile(
		encoding.ReadRootMultiBlockFile(segment))
}

func (buf *SimpleBuffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayVeryLargeFile(
		encoding.ReadRootVeryLargeFile(segment))
}

func (buf *SimpleBuffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))
}

func (buf *SimpleBuffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
