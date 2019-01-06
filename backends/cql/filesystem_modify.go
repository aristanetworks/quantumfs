// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func writeData(fileName string, data []byte) (err error) {

	// We write the data first to a temporary file and then rename it to the
	// passed fileName. This eliminates the chance of reading a partially
	// written file.

	file, err := ioutil.TempFile(filepath.Dir(fileName), filepath.Base(fileName))
	if err != nil {
		return err
	}
	tmpFileName := file.Name()

	_, err = file.Write(data)
	if err != nil {
		file.Close()
		os.Remove(tmpFileName)
		return err
	}

	err = file.Close()
	if err != nil {
		os.Remove(tmpFileName)
		return err
	}

	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		os.Remove(tmpFileName)
		return err
	}

	return nil
}

// Insert will first write the value to the data file and then write the metadata
// file. Retrieval operations will access metadata before data. This ordering is
// imposed so that the existence of the metadata file indicates that the key and its
// value are
// present in the blobstore.
// NOTE: The Insert method has an semaphore limiting the number of
// concurrent Inserts to 100. This limits the number of open files
// opened by the FS implementation of blobstore.
// The number 100, has been emperically determined, and can be changed.
func (b *fileStore) Insert(c ctx, key []byte,
	value []byte, metadata map[string]string) error {

	keyHex := hex.EncodeToString(key)
	defer c.FuncIn("fs::Insert", "key: %s", keyHex).Out()

	dir, filePath := getDirAndFilePath(b, key)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in MkdirAll in Insert %s", err.Error())
	}

	b.sem.P()
	defer b.sem.V()

	err = writeData(filePath+".data", value)
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in writing data in Insert %s", err.Error())
	}

	blobstoreMetadata := make(map[string]interface{})
	t := time.Now()
	blobstoreMetadata["ctime"] = t.Unix()
	blobstoreMetadata["mtime"] = t.Unix()
	blobstoreMetadata["size"] = len(value)

	allMetadata := AllMetadata{BlobStoreMetadata: blobstoreMetadata,
		Metadata: metadata}

	jMetaData, err := json.Marshal(allMetadata)
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in json marshalling of metadata in Insert %s",
			err.Error())
	}

	err = writeData(filePath+".mdata", jMetaData)
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in writing metadata in Insert %s", err.Error())
	}

	return nil
}

func (b *fileStore) Delete(c ctx, key []byte) error {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn("fs::Delete", "key: %s", keyHex).Out()

	_, filePath := getDirAndFilePath(b, key)
	err := os.Remove(filePath + ".mdata")
	if err != nil {
		if os.IsNotExist(err) {
			return NewError(ErrKeyNotFound,
				"key %s not found in Delete", keyHex)
		}
		return NewError(ErrOperationFailed,
			"error in removing metadata in Delete %s", err.Error())
	}

	err = os.Remove(filePath + ".data")
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in removing data in Delete %s", err.Error())
	}
	return nil
}

func (b *fileStore) Update(c ctx, key []byte,
	metadata map[string]string) error {

	keyHex := hex.EncodeToString(key)
	defer c.FuncIn("fs::Update", "key: %s", keyHex).Out()

	_, filePath := getDirAndFilePath(b, key)
	blobstoreMetadata, _, err := retrieveMetadata(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewError(ErrKeyNotFound,
				"key %s not found in Update", keyHex)
		}
		return NewError(ErrOperationFailed,
			"error in retrieve metadata in Update %s", err.Error())
	}
	t := time.Now()
	blobstoreMetadata["mtime"] = t.Unix()

	allMetadata := AllMetadata{BlobStoreMetadata: blobstoreMetadata,
		Metadata: metadata}
	jMetaData, err := json.Marshal(allMetadata)
	err = writeData(filePath+".mdata", jMetaData)
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in writing metadata in Update %s", err.Error())
	}

	return nil
}
