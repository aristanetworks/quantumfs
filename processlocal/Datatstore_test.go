// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

// Unit test to ensure Set() in the package will only upload non-existing data
// to the local datastore.

import "bytes"
import "crypto/sha1"
import "fmt"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/encoding"
import "github.com/aristanetworks/quantumfs/qlog"
import capn "github.com/glycerine/go-capnproto"

// Assert the condition is true. If it is not true then fail the test with the given
// message
func assert(condition bool, format string, args ...interface{}) {
        if !condition {
                msg := fmt.Sprintf(format, args...)
                panic(msg)
        }
}

// Define a dummy buffer type for the testing purpose. Buffer only takes []byte and
// quantumfs.ObjecgtKey 
type buffer_t struct {
        key  quantumfs.ObjectKey
        data []byte
}

func newBuffer(in []byte, q_key quantumfs.ObjectKey) quantumfs.Buffer {
        return &buffer_t{
                key:       q_key,
                data:      in,
        }
}

// Implement the required interface functions. Only  Get() and Set() will be calledd,
// so the others will be briefly implemented or be directly copied from 
// daemon/datastore.go
func (buf *buffer_t) Write(c *quantumfs.Ctx, in []byte, offset uint32) uint32 {
        return uint32(111)
}

func (buf *buffer_t) Read(out []byte, offset uint32) int {
        return int(111)
}

func (buf *buffer_t) Get() []byte {
        return buf.data
}

func (buf *buffer_t) Set(data []byte, keyType quantumfs.KeyType) {
        buf.data = data
}

func (buf *buffer_t) ContentHash() [quantumfs.ObjectKeyLength - 1]byte {
        return sha1.Sum(buf.data)
}

func (buf *buffer_t) Key(c *quantumfs.Ctx) (quantumfs.ObjectKey, error) {
        return buf.key, nil 
}

func (buf *buffer_t) SetSize(size int) {
        buf.data = buf.data[:size]
}

func (buf *buffer_t) Size() int {
        return len(buf.data)
}

func (buf *buffer_t) AsDirectoryEntry() quantumfs.DirectoryEntry {
        segment := capn.NewBuffer(buf.data)
        return quantumfs.OverlayDirectoryEntry(
                encoding.ReadRootDirectoryEntry(segment)) 
}

func (buf *buffer_t) AsWorkspaceRoot() quantumfs.WorkspaceRoot {
        segment := capn.NewBuffer(buf.data)
        return quantumfs.OverlayWorkspaceRoot(
                encoding.ReadRootWorkspaceRoot(segment))
}

func (buf *buffer_t) AsMultiBlockFile() quantumfs.MultiBlockFile {
        segment := capn.NewBuffer(buf.data)
        return quantumfs.OverlayMultiBlockFile(
                encoding.ReadRootMultiBlockFile(segment))
}

func (buf *buffer_t) AsVeryLargeFile() quantumfs.VeryLargeFile {
        segment := capn.NewBuffer(buf.data)
        return quantumfs.OverlayVeryLargeFile(
                encoding.ReadRootVeryLargeFile(segment))
}

func (buf *buffer_t) AsExtendedAttributes() quantumfs.ExtendedAttributes {
        segment := capn.NewBuffer(buf.data)
        return quantumfs.OverlayExtendedAttributes(
                encoding.ReadRootExtendedAttributes(segment))
}
 
// Verify the Set() in processlocal/datastore. With an identical key, the datastore
// shoul only update map once
func TestIdenticalContentSync(t *testing.T) {
        // Initialize a datastore for the test 
        store := NewDataStore("")

        // Define two different content for the same key
        data := []byte("This is a source file")
        data2 :=[]byte("This is a comparison file")
        
        // Create  Ctx with random RequestId
        qlog := qlog.NewQlogTiny()
        requestId := uint64(1000000000)
        ctx := &quantumfs.Ctx{
                Qlog:      qlog,
                RequestId: requestId,          
        }

        // Generate an unique key for Set()
        key_byte := []byte("40123456789abcdefghijklmnopq")
        key := quantumfs.NewObjectKeyFromBytes(key_byte)
        
        // Put the source content into the buffer
        buffer := newBuffer(data, key)
        
        // Set the content with the pre-defined unique key
        store.Set(ctx, key, buffer)
        assert(bytes.Equal(buffer.Get(), data),
                "Error creating incorrcet source buffer: %s\n", buffer.Get())
        
        // Get the content from the datastore
        empty := []byte("                     ")
        output := newBuffer(empty, key)
        store.Get(ctx, key, output)
        
        assert(bytes.Equal(output.Get(), data),
                "Error inserting incorrect data: %s\n",output.Get())

        // Reset the buffer with the same key but different content
        buffer.Set(data2, key.Type())
        assert(bytes.Equal(buffer.Get(), data2),
                "Error creating incorrcet comparison buffer: %s\n", buffer.Get())

        // Verify whether the content for a corresponding key will be
        // overwritten by a new value. The expectation is that content
        // should be intact
        store.Set(ctx, key, buffer)
        store.Get(ctx, key, buffer)
        assert(bytes.Equal(buffer.Get(), data),
                "Error resetting the correct data: %s\n", buffer.Get())
}
