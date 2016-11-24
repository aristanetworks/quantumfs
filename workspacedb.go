// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This is the interface to the Workspace Name database.
package quantumfs

// WorkspaceDB provides a cluster-wide and consistent mapping between names and
// rootids. Workspace names have two components and are represented as strings with
// the format "<namespace>/<workspace>". <namespace> would often be a username or
// something like abuild. <workspace> will be somewhat arbitrary.
//
// Most of the API is synchronous and consistent. The notable exceptions are the
// calls to list the available namespaces and workspaces within a namespace. These
// must respond without blocking from a local cache if possible, perfectly accurate
// data is not required.
type WorkspaceDB interface {

	// These methods need to be instant, but not necessarily completely up to
	// date
	NumNamespaces(c *Ctx) (int, error)
	NamespaceList(c *Ctx) ([]string, error)
	NumWorkspaces(c *Ctx, namespace string) (int, error)
	WorkspaceList(c *Ctx, namespace string) ([]string, error)

	// These methods need to be up to date
	NamespaceExists(c *Ctx, namespace string) (bool, error)
	WorkspaceExists(c *Ctx, namespace string, workspace string) (bool, error)
	Workspace(c *Ctx, namespace string, workspace string) (ObjectKey, error)

	// These methods need to be atomic, but may retry internally
	BranchWorkspace(c *Ctx, srcNamespace string, srcWorkspace string,
		dstNamespace string, dstWorkspace string) error

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
	AdvanceWorkspace(c *Ctx, namespace string, workspace string,
		currentRootId ObjectKey, newRootId ObjectKey) (ObjectKey, error)
}

type WsdbErrCode int
type WorkspaceDbErr struct {
	code WsdbErrCode
}

func NewWorkspaceDbErr(code WsdbErrCode) error {
	return &WorkspaceDbErr{code: code}
}

const (
	WSDB_RESERVED            WsdbErrCode = iota
	WSDB_WORKSPACE_NOT_FOUND             = iota // The workspace didn't exist

	// The operation was based off out of date information
	WSDB_OUT_OF_DATE = iota
)

func (err *WorkspaceDbErr) Error() string {
	switch err.code {
	default:
		return "Unknown wsdb error"
	case WSDB_WORKSPACE_NOT_FOUND:
		return "Workspace not found"
	case WSDB_OUT_OF_DATE:
		return "Workspace changed remotely"
	}
}
