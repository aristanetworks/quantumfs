// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package filesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/errgroup"
)

const fileStoreRoot = "./ocean"

const testKey = "Hello"
const testKeyShaHash = "f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0"
const testValue = "W0rld"
const testKeyDirPath = "./ocean/f7/ff/9"
const unknownKey = "H3llo"

const testKey2 = "D@rth"
const testKey2ShaHash = "d6cefd0925ab07f041135983970231ec7ebdc27d"
const testValue2 = "Vad3r"
const testKey2DirPath = "./ocean/d6/ce/f"
const testKey2BadMetadata = `{"blobstoreMetadata":{"ctime":1,"mtime":1,"size":9},` +
	`"metadata":{"D@rth":"Vad3r"}}`

var testKey2Metadata = map[string]string{"D@rth": "Vad3r"}

var envReady = false
var bls cql.BlobStore

func checkSetup(t *testing.T) {
	if !envReady {
		t.Skip("Env was not setup")
	}
	if bls == nil {
		t.Skip("Blobstore was not setup")
	}
}

//TODO(sid) Tests here are named according to the lexical order and hence assume
// dependency in run order. We'll need to clean up this dependency in a later commit.
func TestEnvSetup(t *testing.T) {

	err := os.MkdirAll(fileStoreRoot, 0777)
	require.NoError(t, err, "FileStoreRoot was not created")
	envReady = true
}

var testCqlCtx = cql.DefaultCtx

func TestNewFilesystemStore(t *testing.T) {

	if !envReady {
		t.Skip("Env was not setup")
	}

	var err error
	bls, err = NewFilesystemStore(fileStoreRoot)
	require.NoError(t, err, "NewFilesystemStore failed")
}

func getFileContents(t *testing.T, fileName string) (os.FileInfo, []byte) {

	// Verify file existence
	fi, err := os.Stat(fileName)
	require.NoError(t, err, "File was not created", fileName)

	// Get contents of file
	f, err := os.Open(fileName)
	require.NoError(t, err, "File could not be opened", fileName)

	b := make([]byte, fi.Size())
	n, err := f.Read(b)
	require.NoError(t, err, "File could not be read", fileName)
	require.Equal(t, fi.Size(), int64(n), "Read fewer bytes than expected ")

	return fi, b
}

func getMetadata(t *testing.T,
	data []byte) (blobstoreMetadata map[string]interface{},
	metadata map[string]string) {

	var allMetadata AllMetadata
	json.Unmarshal(data, &allMetadata)
	return allMetadata.BlobStoreMetadata, allMetadata.Metadata
}

func TestInsert(t *testing.T) {

	insertBeginTime := time.Now().Unix()

	err := bls.Insert(testCqlCtx, []byte(testKey), []byte(testValue), nil)
	require.NoError(t, err, "Insert failed")

	insertEndTime := time.Now().Unix()

	// Verify directory hierarchy
	_, err = os.Stat(testKeyDirPath)
	require.NoError(t, err, "Directory hierarchy was not created")

	filePrefix := fmt.Sprintf("%s/%s", testKeyDirPath, testKeyShaHash)

	// Verify data
	dfi, dcontent := getFileContents(t, filePrefix+".data")
	assert.Equal(t, int64(len(testValue)), dfi.Size(), "File size is incorrect")
	assert.Equal(t, []byte(testValue), dcontent, "File content did not match")

	// Verify metadata
	_, mData := getFileContents(t, filePrefix+".mdata")
	emd, md := getMetadata(t, mData)

	assert.Nil(t, md, "Metadata should be empty")
	fsize := int64(emd["size"].(float64))
	assert.Equal(t, dfi.Size(), fsize,
		"BlobStore Metadata has incorrect file size")
	ctime := int64(emd["ctime"].(float64))
	if ctime < insertBeginTime || ctime > insertEndTime {
		assert.Fail(t, "BlobStore Metadata has incorrect ctime",
			fmt.Sprintf("ctime = %d insertEndTime = %#v "+
				"insertBeginTime = %#v\n",
				ctime, insertEndTime, insertBeginTime))
	}
	mtime := int64(emd["mtime"].(float64))
	assert.Equal(t, ctime, mtime,
		"BlobStore Metadata should have the same ctime and mtime")
}

func TestInsertParallel(t *testing.T) {

	checkSetup(t)
	ctx := context.Background()
	Wg, _ := errgroup.WithContext(ctx)

	for count := 0; count < 2; count++ {
		countl := count
		Wg.Go(func() error {

			return bls.Insert(testCqlCtx,
				[]byte(testKey+strconv.Itoa(countl)),
				[]byte(testValue), nil)
		})
	}
	err := Wg.Wait()
	require.NoError(t, err, "Insert returned an error")

	// Check
	for count := 0; count < 2; count++ {
		value, _, err := bls.Get(testCqlCtx,
			[]byte(testKey+strconv.Itoa(count)))
		require.NoError(t, err, "Get returned an error")
		require.Equal(t, testValue, string(value),
			"Get returned in correct value")
	}
}

func TestGet(t *testing.T) {

	checkSetup(t)

	value, metadata, err := bls.Get(testCqlCtx, []byte(testKey))
	require.NoError(t, err, "Get failed")
	require.Equal(t, testValue, string(value), "Get returned incorrect value")
	require.Equal(t, map[string]string(nil), metadata,
		"Get returned incorrect metadata")

	// Verify return value for a non existent key
	value, metadata, err = bls.Get(testCqlCtx, []byte(unknownKey))
	require.Error(t, err, "Get returned success")
	verr, ok := err.(*cql.Error)
	require.Equal(t, true, ok, fmt.Sprintf("Get incorrect error type %T", err))
	assert.Equal(t, cql.ErrKeyNotFound, verr.Code,
		"Get returned incorrect error %s", verr)
	assert.Nil(t, value, "value was not Nil when error is ErrKeyNotFound")
	assert.Nil(t, metadata, "value was not Nil when error is ErrKeyNotFound")

	// Insert a second key and value and verify Get
	err = bls.Insert(testCqlCtx, []byte(testKey2), []byte(testValue2),
		testKey2Metadata)
	require.NoError(t, err, "Second Insert returned an error")
	value, metadata, err = bls.Get(testCqlCtx, []byte(testKey2))
	require.NoError(t, err, "Second Get returned an error")
	require.Equal(t, testValue2, string(value), "Get returned incorrect value")
	require.Equal(t, testKey2Metadata, metadata,
		"Get returned incorrect metadata")

	// Truncate some bytes from the data file and verify error returned
	dataFile := testKey2DirPath + "/" + testKey2ShaHash + ".data"
	os.Truncate(dataFile, int64(len(testValue2)-1))
	value, metadata, err = bls.Get(testCqlCtx, []byte(testKey2))
	require.Error(t, err, "Get returned incorrect error")
	verr, ok = err.(*cql.Error)
	require.Equal(t, true, ok, fmt.Sprintf("Get incorrect error type %T", err))
	assert.Equal(t, cql.ErrOperationFailed, verr.Code,
		"Get returned incorrect error code")
	assert.Nil(t, value, "value was not Nil when error is ErrOperationFailed")
	assert.Nil(t, metadata, "value was not Nil when error is ErrOperationFailed")

	// Delete data file and verify error returned
	os.Remove(dataFile)
	value, metadata, err = bls.Get(testCqlCtx, []byte(testKey2))
	require.Error(t, err, "Get returned incorrect error")
	verr, ok = err.(*cql.Error)
	require.Equal(t, true, ok, fmt.Sprintf("Get incorrect error type %T", err))
	assert.Equal(t, cql.ErrOperationFailed, verr.Code,
		"Get returned incorrect error code")
	assert.Nil(t, value, "value was not Nil when error is ErrOperationFailed")
	assert.Nil(t, metadata, "value was not Nil when error is ErrOperationFailed")

	// Reinsert the second key and corrupt the metadata to have the wrong file
	// size
	err = bls.Insert(testCqlCtx, []byte(testKey2), []byte(testValue2),
		testKey2Metadata)
	require.NoError(t, err, "Reinsertion of second returned an error")
	value, metadata, err = bls.Get(testCqlCtx, []byte(testKey2))
	require.NoError(t, err, "Get of second returned an error")
	require.Equal(t, testValue2, string(value), "Get returned incorrect value")
	require.Equal(t, testKey2Metadata, metadata,
		"Get returned incorrect metadata")

	mdataFile := testKey2DirPath + "/" + testKey2ShaHash + ".mdata"
	f, _ := os.Create(mdataFile)
	f.Write([]byte(testKey2BadMetadata))
	f.Close()
	value, metadata, err = bls.Get(testCqlCtx, []byte(testKey2))
	require.Error(t, err, "Get returned incorrect error")
	verr, ok = err.(*cql.Error)
	require.Equal(t, true, ok, fmt.Sprintf("Get incorrect error type %T", err))
	assert.Equal(t, cql.ErrOperationFailed, verr.Code,
		"Get returned incorrect error code")
	assert.Nil(t, value, "value was not Nil when error is ErrOperationFailed")
	assert.Nil(t, metadata, "value was not Nil when error is ErrOperationFailed")
}

func TestMetadata(t *testing.T) {

	checkSetup(t)

	metadata, err := bls.Metadata(testCqlCtx, []byte(testKey))
	require.NoError(t, err, "Metadata returned error")
	require.Equal(t, map[string]string(nil), metadata,
		"Metadata returned incorrect metadata")

	// Insert the second key and validate the metadata
	err = bls.Insert(testCqlCtx, []byte(testKey2), nil, testKey2Metadata)
	require.NoError(t, err, "Reinsertion of second metadata returned an error")
	metadata, err = bls.Metadata(testCqlCtx, []byte(testKey2))
	require.NoError(t, err, "Fetching second Metadata returned error")
	require.Equal(t, testKey2Metadata, metadata,
		"Get returned incorrect metadata")

	// verify metadata with unknownKey
	metadata, err = bls.Metadata(testCqlCtx, []byte(unknownKey))
	require.Error(t, err, "Metadata returned success")
	verr, ok := err.(*cql.Error)
	require.Equal(t, true, ok,
		fmt.Sprintf("Metadata returned incorrect error type %T", err))
	assert.Equal(t, cql.ErrKeyNotFound, verr.Code,
		"Metadata returned incorrect error %s", verr)
	assert.Nil(t, metadata, "value was not Nil when error is ErrKeyNotFound")
}

func TestUpdate(t *testing.T) {

	checkSetup(t)

	metadataUpdate := map[string]string{"Luke": "Skywalker"}

	err := bls.Insert(testCqlCtx, []byte(testKey), []byte(testValue), nil)
	require.NoError(t, err, "Insert returned an error")
	err = bls.Update(testCqlCtx, []byte(testKey), metadataUpdate)
	require.NoError(t, err, "Update returned an error")

	value, metadata, err := bls.Get(testCqlCtx, []byte(testKey))
	require.NoError(t, err, "Get returned an error")
	require.Equal(t, []byte(testValue), value,
		"Value after metadata update did not match original")
	require.Equal(t, metadataUpdate, metadata,
		"Metadata after update is incorrect")

	// Verify that mtime gets updated
	testKey2UpdateMetadata := `{"blobstoreMetadata":` +
		`{"ctime":1,"mtime":1,"size":5},"metadata":{"Luk3":"Skywalker"}}`
	mdataFile := testKey2DirPath + "/" + testKey2ShaHash + ".mdata"
	f, err := os.Create(mdataFile)
	require.NoError(t, err, "Create if meta-data file failed")
	f.Write([]byte(testKey2UpdateMetadata))
	f.Close()
	_, mData := getFileContents(t, mdataFile)
	emd, md := getMetadata(t, mData)

	require.Equal(t, "Skywalker", md["Luk3"],
		"Raw write of test metadata failed")
	require.Equal(t, int64(1), int64(emd["mtime"].(float64)),
		"Raw write of test metadata failed")

	updateBeginTime := time.Now().Unix()
	err = bls.Update(testCqlCtx, []byte(testKey2), metadataUpdate)
	require.NoError(t, err, "Update Failed")
	_, mData = getFileContents(t, mdataFile)
	emd, md = getMetadata(t, mData)

	require.Equal(t, "Skywalker", md["Luke"],
		"Update did not overwrite old metadata")
	require.True(t, int64(emd["mtime"].(float64)) >= updateBeginTime,
		"Update did not update mtime in blobstoreMetadata")

	// verify update with unknownKey
	err = bls.Update(testCqlCtx, []byte(unknownKey), nil)
	require.Error(t, err, "Update did not return an error for non existent key")
	verr, ok := err.(*cql.Error)
	require.Equal(t, true, ok,
		fmt.Sprintf("Update returned incorrect error type %T", err))
	assert.Equal(t, cql.ErrKeyNotFound, verr.Code,
		"Update returned incorrect error %s", verr)
}

func TestDelete(t *testing.T) {

	checkSetup(t)

	err := bls.Delete(testCqlCtx, []byte(unknownKey))
	require.Error(t, err, "Delete did not return an error for non existent key")
	verr, ok := err.(*cql.Error)
	require.Equal(t, true, ok,
		fmt.Sprintf("Delete incorrect error type %T", err))
	assert.Equal(t, cql.ErrKeyNotFound, verr.Code,
		"Delete returned incorrect error %s", verr)

	err = bls.Delete(testCqlCtx, []byte(testKey))
	require.NoError(t, err, "Delete returned an error:: ")

	metadataFile := testKeyDirPath + "/" + testKeyShaHash + ".mdata"
	_, err = os.Stat(metadataFile)
	require.True(t, os.IsNotExist(err), "Metadata file was not deleted")
	dataFile := testKeyDirPath + "/" + testKeyShaHash + ".data"
	_, err = os.Stat(dataFile)
	require.True(t, os.IsNotExist(err), "data file was not deleted")
}

func TestMain(m *testing.M) {
	result := m.Run()
	os.RemoveAll(fileStoreRoot)
	os.Exit(result)
}
