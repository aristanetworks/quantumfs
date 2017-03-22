// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

// Unit test to ensure Set() in the package datastore will only upload non-existing
// data to the local datastore.

import "bytes"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/qlog"

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
	requestId := qlog.TestReqId
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestId,
	}

	return ctx
}

// Verify the Set() in processlocal/datastore. With an identical key, the datastore
// should only update map once
func TestIdenticalContentSync(t *testing.T) {
	// Initialize a datastore for the test
	store := NewDataStore("")

	// Define two different contents for the same key
	data := []byte("This is a source file")
	data2 := []byte("This is a comparison file")

	ctx := newCtx()

	// Generate an unique key for Set()
	key_byte := []byte("40123456789abcdefghijklmnopq")
	key := quantumfs.NewObjectKeyFromBytes(key_byte)

	// Put the source content into the buffer
	buffer := quantumfs.NewTestBuffer(data, key)

	// Set the content with the pre-defined unique key
	store.Set(ctx, key, buffer)
	utils.Assert(bytes.Equal(buffer.Get(), data),
		"Error creating incorrect source buffer: %s\n", buffer.Get())

	// Get the content from the datastore
	empty := make([]byte, 32, 32)
	output := quantumfs.NewTestBuffer(empty, key)
	store.Get(ctx, key, output)

	utils.Assert(bytes.Equal(output.Get(), data),
		"Error inserting incorrect data: %s\n", output.Get())

	// Reset the buffer with the same key but a different content
	buffer.Set(data2, key.Type())
	utils.Assert(bytes.Equal(buffer.Get(), data2),
		"Error creating incorrect comparison buffer: %s\n", buffer.Get())

	// Verify whether the content for a corresponding key will be
	// overwritten by a new value. The expectation is that content
	// should be unchanged in the datastore
	store.Set(ctx, key, buffer)
	store.Get(ctx, key, buffer)
	utils.Assert(bytes.Equal(buffer.Get(), data),
		"Error resetting the correct data: %s\n", buffer.Get())
}
