// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package blobstore provides an interface for a key-value store where the key is
// typically content hash. A backend data store (eg: filesystem, CQL based store etc)
// can implement the blobstore interface. It assumes that a given key refers to an
// unique value and clients do not provide multiple values for the same key. It also
// stores meta data for the blobs which are passed back and forth as a map of name
// and value pairs. The blobstore makes no assumptions about the data and metadata
// being stored in there and treats them as bytes and strings respectively.
package cql

import ()

// BSErrorCode are integer codes for error responses from cql/blobstore package
type BSErrorCode int

// // Different error code exported by blobstore package
// const (
// 	ErrReserved              BSErrorCode = iota // Reserved since 0
// 	ErrOperationFailed       BSErrorCode = iota // The specific operation failed
// 	ErrBlobStoreDown         BSErrorCode = iota // bs could not be reached
// 	ErrBlobStoreInconsistent BSErrorCode = iota // bs has an internal error
// 	ErrBadArguments          BSErrorCode = iota // arguments are incorrect
// 	ErrKeyNotFound           BSErrorCode = iota // key and value was not found
// )

// Error implements error interface and encapsulates the error returned by
// cql/blobstore package's APIs
// type Error struct {
// 	Code ErrorCode // This can be used as a sentinal value
// 	Msg  string      // This is for human eyes only
// }

// func (err Error) Error() string {
// 	return fmt.Sprintf("blobstore.Error %d (%s)", err.Code, err.Msg)
// }

// // NewError returns a new error
// func NewError(code ErrorCode, msg string, a ...interface{}) error {
// 	return &Error{Code: code, Msg: fmt.Sprintf(msg, a...)}
// }

// BlobStore interface
type BlobStore interface {

	// Get returns both the value and metadata for the given key or
	// an error
	Get(c Ctx, key []byte) ([]byte, map[string]string, error)

	// Insert stores the given value and metadata for the key
	// Calling Insert with a pre-existing key will overwrite the
	// previous value and metadata. Metadata items are namespaced to
	// distinguish between store specific metadata and application
	// specific metadata. Example: "TimeToLive" is a CQL store
	// specific metadata which represents the seconds after which
	// the blob should be cleaned up by the system. Refer to store
	// specific documentation to see the store specific metadata items.
	Insert(c Ctx, key []byte, value []byte,
		metadata map[string]string) error

	// Delete removes both the value and metadata for a given key from the
	// blobstore.
	Delete(c Ctx, key []byte) error

	// Metadata returns the metadata for the given key or an error
	Metadata(c Ctx, key []byte) (map[string]string, error)

	// Update updates the metadata for a given key irrespective of its prior
	// existence. If the key does not exist the value associated with it will
	// be an empty blob or string.
	Update(c Ctx, key []byte, metadata map[string]string) error
}
