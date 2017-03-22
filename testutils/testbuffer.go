// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package testutils

// This file contains a buffer for the testing purpose only. Some interface functions
// are only briefly implemented with a dummy return value

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/hash"
import capn "github.com/glycerine/go-capnproto"

// Testing buffer only contains data and key to meet the requirements of Set() and
// Get() in datastore
type TestBuffer struct {
	key  quantumfs.ObjectKey
	data []byte
}

func NewTestBuffer(in []byte, q_key quantumfs.ObjectKey) quantumfs.Buffer {
	return &TestBuffer{
		key:  q_key,
		data: in,
	}
}

// Implement the required interface functions. Only  Get() and Set() will be called,
// so the others will be briefly implemented or be directly copied from
// daemon/datastore.go
func (buf *TestBuffer) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
	panic("Error: The Write function of TestBuffer is not implemented")
}

func (buf *TestBuffer) Read(out []byte, offset uint32) int {
	panic("Error: The Read function of TestBuffer is not implemented")
}

func (buf *TestBuffer) Get() []byte {
	return buf.data
}

func (buf *TestBuffer) Set(data []byte, keyType quantumfs.KeyType) {
	buf.data = data
}

func (buf *TestBuffer) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
	return hash.Hash(buf.data)
}

func (buf *TestBuffer) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
	return buf.key, nil
}

func (buf *TestBuffer) SetSize(size int) {
	buf.data = buf.data[:size]
}

func (buf *TestBuffer) Size() int {
	return len(buf.data)
}

func (buf *TestBuffer) AsDirectoryEntry() quantumfs.DirectoryEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayDirectoryEntry(encoding.ReadRootDirectoryEntry(segment))
}

func (buf *TestBuffer) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayWorkspaceRoot(encoding.ReadRootWorkspaceRoot(segment))
}

func (buf *TestBuffer) AsMultiBlockFile() quantumfs.MultiBlockFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayMultiBlockFile(encoding.ReadRootMultiBlockFile(segment))
}

func (buf *TestBuffer) AsVeryLargeFile() quantumfs.VeryLargeFile {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayVeryLargeFile(encoding.ReadRootVeryLargeFile(segment))
}

func (buf *TestBuffer) AsExtendedAttributes() quantumfs.ExtendedAttributes {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayExtendedAttributes(
		encoding.ReadRootExtendedAttributes(segment))
}

func (buf *TestBuffer) AsHardlinkEntry() quantumfs.HardlinkEntry {
	segment := capn.NewBuffer(buf.data)
	return quantumfs.OverlayHardlinkEntry(
		encoding.ReadRootHardlinkEntry(segment))

}
