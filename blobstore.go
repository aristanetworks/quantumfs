// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package blobstore implements a key value blobstore for data that is usually
// referenced by a content hash. It implements multiple backends to store data
// like a filesystem, scylladb, etc.
package blobstore

import (
	"fmt"
)

// BSErrorCode are intger codes for error responses from ether package
type BSErrorCode int

// Different error code exported by blobstore package
const (
	ErrReserved              BSErrorCode = iota // Reserved since 0
	ErrOperationFailed       BSErrorCode = iota // The specific operation failed
	ErrBlobStoreDown         BSErrorCode = iota // The blobstore could not be reached
	ErrBlobStoreInconsistent BSErrorCode = iota // The blobstore has an internal error
	ErrBadArguments          BSErrorCode = iota // The passed arguments are incorrect
	ErrKeyNotFound           BSErrorCode = iota // The key and associated value was not found
)

// Error implements error interface and encapsulates the error returned by ether package's APIs
type Error struct {
	Code BSErrorCode // This can be used as a sentinal value
	Msg  string      // This is for human eyes only
}

func (err Error) Error() string {
	return fmt.Sprintf("blobstore.Error %d (%s)", err.Code, err.Msg)
}

// NewError returns a new error
func NewError(code BSErrorCode, msg string, a ...interface{}) error {
	return &Error{Code: code, Msg: fmt.Sprintf(msg, a...)}
}

// BlobStore stores blobs of data that are referenced by a key which typically
// includes the content hash of the value. It assumes that a given key refers to an
// unique value and clients do not provide multiple values for the same key. It also
// stores meta data for the blobs which are passed back and forth as a map of name
// and value pairs. The blobstore makes no assumptions about the data and metadata
// being stored in there and treats them as bytes and strings respectively.
type BlobStore interface {

	// Get returns both the value and metadata for the given key along
	// with an error value that indicates whether the key was found.
	Get(key string) ([]byte, map[string]string, error)

	// Insert stores the given value and metadata for the key
	// Calling Insert with a pre-existing key will overwrite the
	// previous value and metadata.
	Insert(key string, value []byte, metadata map[string]string) error

	// Delete removes both the value and metadata for a given key from the
	// blobstore.
	Delete(key string) error

	// Metadata returns the metadata for the given key along with a boolean
	// value that indicates whether the key was found.
	Metadata(key string) (map[string]string, error)

	// Update updates the metadata for a given key irrespective of its prior
	// existence. If the key does not exist the value associated with it will
	// be an empty blob or string.
	Update(key string, metadata map[string]string) error
}
