// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

	"github.com/aristanetworks/ether"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testKey = "Hello"
const testValue = "W0rld"
const unknownKey = "H3llo"
const testKey2 = "D@rth"
const testValue2 = "Vad3r"

var testKey2Metadata = map[string]string{}
var bls ether.BlobStore

func checkSetup(t *testing.T) {
	if bls == nil {
		t.Skip("Blobstore was not setup")
	}
}

// TODO:(sid) Caveat is that currently the test targets a "shared" cluster
// and expects that the keyspace and table are already setup.
// Once the integration tests are managed using jenkins and containers,
// the test should be modified to run against its "private" cluster instance
func TestNewCqlStore(t *testing.T) {

	var err ether.ErrorResponse
	name := "../clusterConfigs/hwConfig"
	bls, err = NewCqlBlobStore(name)
	require.Equal(t, ether.ErrOk, err.ErrorCode,
		"NewFilesystemStore returned an error:: "+err.Error())
}

func TestInsert(t *testing.T) {
	checkSetup(t)
	err := bls.Insert(testKey, []byte(testValue), nil)
	require.Equal(t, ether.ErrOk, err.ErrorCode,
		"Insert returned an error:: "+err.Error())
}

func TestGet(t *testing.T) {
	checkSetup(t)
	value, metadata, err := bls.Get(testKey)
	require.Equal(t, ether.ErrOk, err.ErrorCode,
		"Get returned an error::  "+err.Error())
	require.Equal(t, testValue, string(value), "Get returned incorrect value")
	require.Equal(t, map[string]string(nil), metadata,
		"Get returned incorrect metadata")

	// Verify return value for a non existent key
	value, metadata, err = bls.Get(unknownKey)
	assert.Nil(t, value, "value was not Nil when error is ErrKeyNotFound")
	assert.Nil(t, metadata, "value was not Nil when error is ErrKeyNotFound")
	require.Equal(t, ether.ErrKeyNotFound, err.ErrorCode,
		"Get returned incorrect error")

	// Insert a second key and value and verify Get
	err = bls.Insert(testKey2, []byte(testValue2), testKey2Metadata)
	require.Equal(t, ether.ErrOk, err.ErrorCode,
		"Second Insert returned an error:: "+err.Error())
	value, metadata, err = bls.Get(testKey2)
	require.Equal(t, testValue2, string(value), "Get returned incorrect value")
	//require.Equal(t, testKey2Metadata, metadata,
	//	"Get returned incorrect metadata")
	resetCqlStore() //TODO: For now just resetting in the last test
}
