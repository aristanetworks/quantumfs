// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package filesystem implements an cql.blobstore interface
// on a locally accessible filesystem
package cql

import (
	"crypto/sha1"
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs"
)

type fileStore struct {
	root string
	sem  Semaphore
}

// AllMetadata is the blobstore metadata for a block of data
type AllMetadata struct {
	BlobStoreMetadata map[string]interface{} `json:"blobstoremetadata"`
	Metadata          map[string]string      `json:"metadata"`
}

func getDirAndFilePath(b *fileStore, key []byte) (dir string, filePath string) {

	hash := sha1.Sum(key)
	dir = fmt.Sprintf("%s/%x/%x/%x", b.root, hash[0], hash[1], hash[2]/16)
	filePath = fmt.Sprintf("%s/%x", dir, hash)
	return dir, filePath
}

//NewFilesystemStore allocats a new blobstore.datastore using local FS as
// backend store
func NewFilesystemStore(path string) (BlobStore, error) {
	var store fileStore

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, NewError(ErrOperationFailed,
			"path does not exist: %v", err)
	}

	if !fileInfo.Mode().IsDir() {
		return nil, NewError(ErrOperationFailed,
			"path is not a dir: %v", path)
	}

	store.root = path
	store.sem = make(Semaphore, 100)
	return &store, nil
}

// TODO(krishna) TTL configuration is specific to CQL
// However due to current blobstore APIs managing store specific
// metadata in common APIs, TTL metadata is being applied to all
// blobstores managed by cql adapter.
// APIs will be refactored to support store specific interfaces
// for managing store specific metadata
//
// Currently, filesystem datastore doesn't accept a configuration file.
// Hence refreshTTLTimeSecs = refreshTTLValueSecs =  defaultTTLValueSecs = 0
// Hence the TTL metadata defaults to 0. In filesystem
// datastore the TTL on the block doesn't count down and hence TTL is
// actually never refreshed since TTL > refreshTTLTimeSecs (=0) always

func NewCqlFilesystemStore(path string) quantumfs.DataStore {
	bs, err := NewFilesystemStore(path)
	if err != nil {
		fmt.Printf("Failed to init cql.filesystem datastore: %s\n",
			err.Error())
		return nil
	}
	translator := CqlBlobStoreTranslator{Blobstore: bs}
	return &translator
}
