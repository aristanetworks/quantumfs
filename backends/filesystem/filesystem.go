// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package filesystem implements an ether.blobstore interface
// on a locally accessible filesystem
package filesystem

import (
	"crypto/sha1"
	"fmt"
	"os"

	blobstore "github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/backends/cql/utils"
)

type fileStore struct {
	root string
	sem  utils.Semaphore
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

//NewFilesystemStore allocats a new blobstore.datastore using local FS as backend store
func NewFilesystemStore(path string) (blobstore.BlobStore, error) {
	var store fileStore

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "path does not exist: %v", err)
	}

	if !fileInfo.Mode().IsDir() {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "path is not a dir: %v", path)
	}

	store.root = path
	store.sem = make(utils.Semaphore, 100)
	return &store, nil
}
