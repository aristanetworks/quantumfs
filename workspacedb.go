// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This is the interface to the Workspace Name database.
package quantumfs

import "fmt"

// Nonce is a number used to distinguish between workspaces of the same path, but
// different lifetimes. For example, if a workspace path were deleted and then
// recreated, the old workspace and new workspace would have different Nonces and
// therefore be distinguishable.
type Nonce uint64

// WorkspaceDB provides a cluster-wide and consistent mapping between names and
// rootids. Workspace names have two components and are represented as strings with
// the format "<typespace>/<namespace>/<workspace>". <typespace> would often be a
// type such as abuild, <namespace> would often be a branch under typespace such as
// eos-trunk. <workspace> will be somewhat arbitrary.
//
// With different typespaces, its namespace and workspace can vary:
//	abuild/eos-trunk/eos-trunk@12345
//	users/joe/arbitrary
//	releases/eos/EOS-4.25.6
//
// Most of the API is synchronous and consistent. The notable exceptions are the
// calls to list the available namespaces and workspaces within a namespace. These
// must respond without blocking from a local cache if possible, perfectly accurate
// data is not required.
type WorkspaceDB interface {

	// These methods need to be instant, but not necessarily perfectly up to
	// date. Positive caching over short periods, such as one second, is
	// acceptable. Negative caching is not acceptable.
	NumTypespaces(c *Ctx) (int, error)
	TypespaceList(c *Ctx) ([]string, error)
	NumNamespaces(c *Ctx, typespace string) (int, error)
	NamespaceList(c *Ctx, typespace string) ([]string, error)
	NumWorkspaces(c *Ctx, typespace string, namespace string) (int, error)
	WorkspaceList(c *Ctx, typespace string, namespace string) (map[string]Nonce,
		error)

	// These methods need to be up to date
	Workspace(c *Ctx, typespace string, namespace string,
		workspace string) (ObjectKey, Nonce, error)

	// These methods need to be atomic, but may retry internally
	BranchWorkspace(c *Ctx, srcTypespace string, srcNamespace string,
		srcWorkspace string, dstTypespace string, dstNamespace string,
		dstWorkspace string) error

	DeleteWorkspace(c *Ctx, typespace string, namespace string,
		workspace string) error

	SetWorkspaceImmutable(c *Ctx, typespace string, namespace string,
		workspace string) error

	WorkspaceIsImmutable(c *Ctx, typespace string, namespace string,
		workspace string) (bool, error)

	// AdvanceWorkspace changes the workspace rootID. If the current rootID
	// doesn't match what the client considers the current rootID then no changes
	// are made and the current rootID is returned such that the client can merge
	// the changes and retry.
	//
	// The return value is the new rootID.
	//
	// Possible errors are:
	// WSDB_WORKSPACE_NOT_FOUND: The named workspace didn't exist
	// WSDB_OUT_OF_DATE: The workspace rootID was changed remotely so the local
	//                   instance is out of date.
	AdvanceWorkspace(c *Ctx, typespace string, namespace string,
		workspace string, nonce Nonce, currentRootId ObjectKey,
		newRootId ObjectKey) (ObjectKey, error)
}

type WsdbErrCode int
type WorkspaceDbErr struct {
	Code WsdbErrCode
	Msg  string
}

func NewWorkspaceDbErr(code WsdbErrCode, format string,
	args ...interface{}) error {

	return &WorkspaceDbErr{Code: code, Msg: fmt.Sprintf(format, args...)}
}

const (
	WSDB_RESERVED            WsdbErrCode = 0
	WSDB_WORKSPACE_NOT_FOUND             = 1 // The workspace didn't exist
	WSDB_WORKSPACE_EXISTS                = 2 // The workspace already exists
	WSDB_FATAL_DB_ERROR                  = 3 // Fatal error in workspace DB
	WSDB_LOCKED                          = 4 // workspace is locked
	// The operation was based off out of date information
	WSDB_OUT_OF_DATE = 5
)

func (err WorkspaceDbErr) Error() string {
	return fmt.Sprintf("%s : %s", err.ErrorCode(), err.Msg)
}

func (err *WorkspaceDbErr) ErrorCode() string {
	switch err.Code {
	default:
		return "Unknown wsdb error"
	case WSDB_WORKSPACE_EXISTS:
		return "Workspace already exists"
	case WSDB_WORKSPACE_NOT_FOUND:
		return "Workspace not found"
	case WSDB_FATAL_DB_ERROR:
		return "Fatal error in workspace DB"
	case WSDB_OUT_OF_DATE:
		return "Workspace changed remotely"
	case WSDB_LOCKED:
		return "Workspace lock error"
	}
}
