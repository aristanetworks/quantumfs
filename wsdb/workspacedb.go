// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package wsdb is the interface to the Workspace Name database.
package wsdb

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/aristanetworks/ether"
)

// ObjectKey is key used to access object in the cluster-wide
// object store using Ether's BlobStore API
type ObjectKey []byte

// WorkspaceNonce is a number used to distinguish between workspaces of the same
// path, but different lifetimes. For example, if a workspace path were deleted and
// then recreated, the old workspace and new workspace would have different
// WorkspaceNonces and therefore be distinguishable.
type WorkspaceNonce int64

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
	ErrFatal = iota
	// ErrWorkspaceExists means that the workspace already exists
	ErrWorkspaceExists = iota
	// ErrWorkspaceNotFound means that the workspace doesn't exist
	ErrWorkspaceNotFound = iota

	// ErrWorkspaceOutOfDate means the operation was based on
	// out of date information. This error occurs only when
	// concurrent updates to same workspace is supported
	// from different cluster nodes
	ErrWorkspaceOutOfDate = iota

	// ErrLocked means that typespace or namespace or workspace
	// is locked for mutable operations like Branch or Advance.
	// Example: a Locked typespace implies that it cannot be
	// use as a destination typespace in Branch operation.
	ErrLocked = iota
)

// WorkspaceNonceInvalid is an invalid nonce
const WorkspaceNonceInvalid = WorkspaceNonce(0)

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
	return strconv.FormatInt(int64(*nonce), 10)
}

// Value returns the int64 representation for WorkspaceNonce
func (nonce *WorkspaceNonce) Value() int64 {
	return int64(*nonce)
}

// StringToNonce returns WorkspaceNonce for a given string
func StringToNonce(nonceStr string) (WorkspaceNonce, error) {
	num, err := strconv.ParseInt(nonceStr, 10, 64)
	if err != nil {
		return WorkspaceNonce(0), err
	}
	return WorkspaceNonce(num), nil
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
	NumTypespaces(c ether.Ctx) (int, error)
	TypespaceList(c ether.Ctx) ([]string, error)
	NumNamespaces(c ether.Ctx, typespace string) (int, error)
	NamespaceList(c ether.Ctx, typespace string) ([]string, error)
	NumWorkspaces(c ether.Ctx, typespace string, namespace string) (int, error)
	WorkspaceList(c ether.Ctx, typespace string,
		namespace string) (map[string]WorkspaceNonce, error)

	// These methods need to be up to date
	Workspace(c ether.Ctx, typespace string, namespace string,
		workspace string) (ObjectKey, WorkspaceNonce, error)

	// These methods need to be atomic, but may retry internally

	// CreateWorkspace creates typespace/namespace/workspace workspace
	// with the key as wsKey. There are no checks to see if the workspace
	// already exists. To be used from only inside init functions of clients of
	// etherWorkspaceDB.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	CreateWorkspace(c ether.Ctx, typespace string, namespace string,
		workspace string, nonce WorkspaceNonce, wsKey ObjectKey) error

	// BranchWorkspace branches srcTypespace/srcNamespace/srcWorkspace to create
	// dstTypespace/dstNamespace/dstWorkspace.
	// srcNonce and dstNonce is returned so it can inserted in the cache.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	//  ErrWorkspaceNotFound
	BranchWorkspace(c ether.Ctx, srcTypespace string, srcNamespace string,
		srcWorkspace string, dstTypespace string,
		dstNamespace string, dstWorkspace string) (WorkspaceNonce, WorkspaceNonce, error)

	// DeleteWorkspace deletes the workspace
	DeleteWorkspace(c ether.Ctx, typespace string, namespace string,
		workspace string) error

	// WorkspaceLastWriteTime returns the time when the workspace DB entry
	// was created or when the rootID was advanced. The time returned is in UTC.
	WorkspaceLastWriteTime(c ether.Ctx, typespace string, namespace string,
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
	//  	remotely so the local instance is out of date
	AdvanceWorkspace(c ether.Ctx, typespace string, namespace string, workspace string,
		nonce WorkspaceNonce, currentRootID ObjectKey, newRootID ObjectKey) (ObjectKey, error)
}
