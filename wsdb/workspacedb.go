// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package wsdb is the interface to the Workspace Name database.
package wsdb

import "fmt"

// ObjectKey is key used to access object in the cluster-wide
// object store using Ether's BlobStore API
type ObjectKey []byte

// ErrCode is error code from workspace DB APIs
type ErrCode int

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

// WorkspaceDB provides a cluster-wide and consistent mapping between names and
// ObjectKeys. Workspace names have two components and are represented as strings with
// the format "<namespace>/<workspace>". <namespace> would often be a username or
// something like abuild. <workspace> will be somewhat arbitrary.
//
// Most of the API is synchronous and consistent. The notable exceptions are the
// calls to list the available namespaces and workspaces within a namespace. These
// must respond without blocking from a local cache if possible, perfectly accurate
// data is not required.
//
// Since workspaceDB is a cluster-wide datastore, apart from errors like
// ErrWorkspaceNotFound, ErrWorkspaceExists, it is possible to get
// ErrFatal due to unrecoverable cluster issues
type WorkspaceDB interface {

	// These methods need to be instant, but not necessarily completely up to
	// date
	NumTypespaces() (int, error)
	TypespaceList() ([]string, error)
	NumNamespaces(typespace string) (int, error)
	NamespaceList(typespace string) ([]string, error)
	NumWorkspaces(typespace string, namespace string) (int, error)
	WorkspaceList(typespace string, namespace string) ([]string, error)

	// These methods need to be up to date
	TypespaceExists(typespace string) (bool, error)
	NamespaceExists(typespace string, namespace string) (bool, error)
	WorkspaceExists(typespace string, namespace string,
		workspace string) (bool, error)
	Workspace(typespace string, namespace string,
		workspace string) (ObjectKey, error)

	// These methods need to be atomic, but may retry internally

	// BranchWorkspace branches srcNamespace/srcWorkspace to create
	// dstNamespace/dstWorkspace
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	//  ErrWorkspaceNotFound
	BranchWorkspace(srcTypespace string, srcNamespace string, srcWorkspace string,
		dstTypespace string, dstNamespace string, dstWorkspace string) error

	// AdvanceWorkspace changes the workspace rootID. If the current rootID
	// doesn't match what the client considers the current rootID then no changes
	// are made and the current rootID is returned such that the client can merge
	// the changes and retry.
	//
	// The return value is the new rootID.
	//
	// Possible errors are:
	//  ErrWorkspaceNotFound
	//  ErrWorkspaceOutOfDate: The workspace rootID was changed remotely so the local
	//                         instance is out of date
	AdvanceWorkspace(typespace string, namespace string, workspace string,
		currentRootID ObjectKey, newRootID ObjectKey) (ObjectKey, error)
}
