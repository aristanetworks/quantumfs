// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package wsdb is the interface to the Workspace Name database.
package cql

import (
	"encoding/hex"
	"fmt"
	"time"
)

// ObjectKey is key used to access object in the cluster-wide
// object store using Cql's BlobStore API
type ObjectKey []byte

// WorkspaceNonce is a number used to distinguish between workspaces of the same
// path, but different lifetimes. For example, if a workspace path were deleted and
// then recreated, the old workspace and new workspace would have different
// WorkspaceNonces and therefore be distinguishable.
type WorkspaceNonce struct {
	Id          int64
	PublishTime int64
}

func (key ObjectKey) String() string {
	return hex.EncodeToString(key)
}

// ErrCode is error code from workspace DB APIs
type ErrCode int

// NullSpaceName is the string token for each part of the null workspace path
const NullSpaceName = "_"

const (
	// ErrReserved should not be used
	ErrReserved ErrCode = iota
	// ErrFatal means API has encountered a fatal error
	ErrFatal ErrCode = iota
	// ErrWorkspaceExists means that the workspace already exists
	ErrWorkspaceExists ErrCode = iota
	// ErrWorkspaceNotFound means that the workspace doesn't exist
	ErrWorkspaceNotFound ErrCode = iota

	// ErrWorkspaceOutOfDate means the operation was based on
	// out of date information. This error occurs only when
	// concurrent updates to same workspace is supported
	// from different cluster nodes
	ErrWorkspaceOutOfDate ErrCode = iota

	// ErrLocked means that typespace or namespace or workspace
	// is locked for mutable operations like Branch or Advance.
	// Example: a Locked typespace implies that it cannot be
	// use as a destination typespace in Branch operation.
	ErrLocked ErrCode = iota

	ErrOperationFailed       ErrCode = iota // The specific operation failed
	ErrBlobStoreDown         ErrCode = iota // The blobstore could not be reached
	ErrBlobStoreInconsistent ErrCode = iota // The blobstore has an internal err
	ErrBadArguments          ErrCode = iota // The passed arguments are incorrect
	ErrKeyNotFound           ErrCode = iota // The key and value was not found
)

// WorkspaceNonceInvalid is an invalid nonce
var WorkspaceNonceInvalid = WorkspaceNonce{0, 0}

// Error represents the error returned from workspace DB APIs
type Error struct {
	Code ErrCode // This can be used as sentinal value
	Msg  string  // This is for humans
}

// NewError returns a new error
func NewError(code ErrCode, msg string, args ...interface{}) error {
	return &Error{Code: code, Msg: fmt.Sprintf(msg, args...)}
}

// ErrorCode returns a readable string for ErrCode
func (err *Error) ErrorCode() string {
	switch err.Code {
	default:
		return "Unknown wsdb error"
	case ErrWorkspaceExists:
		return "Workspace already exists"
	case ErrWorkspaceNotFound:
		return "Workspace not found"
	case ErrFatal:
		return "Fatal workspaceDB error"
	case ErrWorkspaceOutOfDate:
		return "Workspace changed remotely"
	case ErrLocked:
		return "Lock error"
	}
}

func (err *Error) Error() string {
	return fmt.Sprintf("Error: %s: %s", err.ErrorCode(), err.Msg)
}

// String returns the string representation for WorkspaceNonce
func (nonce *WorkspaceNonce) String() string {
	return fmt.Sprintf("%d %d", nonce.Id, nonce.PublishTime)
}

// SameIncarnation returns true if nonce and other are the same incarnation of
// a workspace
func (nonce *WorkspaceNonce) SameIncarnation(other *WorkspaceNonce) bool {
	return nonce.Id == other.Id
}

// StringToNonce returns WorkspaceNonce for a given string
func StringToNonce(nonceStr string) (WorkspaceNonce, error) {
	var id, publishTime int64
	n, err := fmt.Sscan(nonceStr, &id, &publishTime)
	if err != nil {
		return WorkspaceNonceInvalid, err
	}
	if n != 2 {
		return WorkspaceNonceInvalid, fmt.Errorf("Parsed %d elements", n)
	}
	return WorkspaceNonce{id, publishTime}, nil
}

// WorkspaceDB provides a cluster-wide and consistent mapping between names
// and ObjectKeys. Workspace names have three components and are represented
// as strings with the format "<typespace>/<namespace>/<workspace>".
//
// Most of the API is synchronous and consistent. The notable exceptions are
// the calls to list and count of typespaces, namespaces and workspaces.
// These must respond without blocking, from a local cache if possible,
// perfectly accurate data is not required.
//
// Since workspaceDB is a cluster-wide datastore, apart from errors like
// ErrWorkspaceNotFound, ErrWorkspaceExists, it is possible to get
// ErrFatal due to unrecoverable cluster issues
type WorkspaceDB interface {

	// These methods need to be instant, but not necessarily completely up to
	// date
	NumTypespaces(c ctx) (int, error)
	TypespaceList(c ctx) ([]string, error)
	NumNamespaces(c ctx, typespace string) (int, error)
	NamespaceList(c ctx, typespace string) ([]string, error)
	NumWorkspaces(c ctx, typespace string, namespace string) (int, error)
	WorkspaceList(c ctx, typespace string,
		namespace string) (map[string]WorkspaceNonce, error)

	// These methods need to be up to date
	Workspace(c ctx, typespace string, namespace string,
		workspace string) (ObjectKey, WorkspaceNonce, error)

	// These methods need to be atomic, but may retry internally

	// CreateWorkspace creates typespace/namespace/workspace workspace
	// with the key as wsKey. There are no checks to see if the workspace
	// already exists. To be used from only inside init functions of clients of
	// cqlWorkspaceDB.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	CreateWorkspace(c ctx, typespace string, namespace string,
		workspace string, nonce WorkspaceNonce, wsKey ObjectKey) error

	// BranchWorkspace branches srcTypespace/srcNamespace/srcWorkspace to create
	// dstTypespace/dstNamespace/dstWorkspace.
	// srcNonce and dstNonce is returned so it can inserted in the cache.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	//  ErrWorkspaceNotFound
	BranchWorkspace(c ctx, srcTypespace string, srcNamespace string,
		srcWorkspace string, dstTypespace string,
		dstNamespace string, dstWorkspace string) (WorkspaceNonce,
		WorkspaceNonce, error)

	// DeleteWorkspace deletes the workspace
	DeleteWorkspace(c ctx, typespace string, namespace string,
		workspace string) error

	// WorkspaceLastWriteTime returns the time when the workspace DB entry
	// was created or when the rootID was advanced. The time returned is in UTC.
	WorkspaceLastWriteTime(c ctx, typespace string, namespace string,
		workspace string) (time.Time, error)

	// AdvanceWorkspace changes the workspace rootID. If the
	// current rootID doesn't match what the client considers
	// the current rootID then no changes are made and the
	// current rootID is returned such that the client can
	// merge the changes and retry.
	//
	// The return value is the new rootID.
	//
	// Possible errors are:
	//  ErrWorkspaceNotFound
	//  ErrWorkspaceOutOfDate: The workspace rootID was changed
	//  remotely so the local instance is out of date
	AdvanceWorkspace(c ctx, typespace string, namespace string, workspace string,
		nonce WorkspaceNonce, currentRootID ObjectKey,
		newRootID ObjectKey) (ObjectKey, WorkspaceNonce, error)

	SetWorkspaceImmutable(c ctx, typespace string, namespace string,
		workspace string) error

	WorkspaceIsImmutable(c ctx, typespace string, namespace string,
		workspace string) (bool, error)
}
