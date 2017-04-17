// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package simplebuffer

// This file contains a buffer for use in testing, walker, and uploader.
// Some interface functions are only briefly implemented with a dummy return value.

import "fmt"
import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/hash"
import capn "github.com/glycerine/go-capnproto"

// Buffer only contains data and key to meet the requirements of Set() and
// Get() in datastore. This can be used in tests and tools which need to
// use datastore API and thus need an implementation of quantumfs.Buffer
// interface
type Buffer struct {
	key  quantumfs.ObjectKey
	data []byte
}

func New(in []byte, q_key quantumfs.ObjectKey) quantumfs.Buffer {
	return &Buffer{
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
func (buf *Buffer) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
	panic("Error: The Write function of Buffer is not implemented")
}

func (buf *Buffer) Read(out []byte, offset uint32) int {
	panic("Error: The Read function of Buffer is not implemented")
}

func (buf *Buffer) Get() []byte {
	return buf.data
}

func (buf *Buffer) Set(data []byte, keyType quantumfs.KeyType) {
	buf.data = data
}

func (buf *Buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *Buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	return buf.key, nil
}

func (buf *Buffer) SetSize(size int) {
	buf.data = buf.data[:size]
}

func (buf *Buffer) Size() int {
	return len(buf.data)
}

func (buf *Buffer) AsDirectoryEntry() quantumfs.DirectoryEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayDirectoryEntry(
		encoding.ReadRootDirectoryEntry(segment))
}

func (buf *Buffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayWorkspaceRoot(
		encoding.ReadRootWorkspaceRoot(segment))
}

func (buf *Buffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayMultiBlockFile(
		encoding.ReadRootMultiBlockFile(segment))
}

func (buf *Buffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayVeryLargeFile(
		encoding.ReadRootVeryLargeFile(segment))
}

func (buf *Buffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))
}

func (buf *Buffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
