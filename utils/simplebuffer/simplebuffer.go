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
type buffer struct {
	key  quantumfs.ObjectKey
	data []byte
}

func New(in []byte, q_key quantumfs.ObjectKey) quantumfs.Buffer {
	return &buffer{
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
func (buf *buffer) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
	panic("Error: The Write function of Buffer is not implemented")
}

func (buf *buffer) Read(out []byte, offset uint32) int {
	panic("Error: The Read function of Buffer is not implemented")
}

func (buf *buffer) Get() []byte {
	return buf.data
}

func (buf *buffer) Set(data []byte, keyType quantumfs.KeyType) {
	buf.data = data
}

func (buf *buffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *buffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	return buf.key, nil
}

func (buf *buffer) SetSize(size int) {
	buf.data = buf.data[:size]
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
