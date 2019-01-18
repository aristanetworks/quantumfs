// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package wsdb is the interface to the Workspace Name database.
package cql

import (
	"encoding/hex"
	"time"

	"github.com/aristanetworks/quantumfs"
)

// ObjectKey is key used to access object in the cluster-wide
// object store using Cql's BlobStore API
type ObjectKey []byte

func (key ObjectKey) String() string {
	return hex.EncodeToString(key)
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
		namespace string) (map[string]quantumfs.WorkspaceNonce, error)

	// These methods need to be up to date
	Workspace(c ctx, typespace string, namespace string,
		workspace string) (ObjectKey, quantumfs.WorkspaceNonce, error)

	// These methods need to be atomic, but may retry internally

	// CreateWorkspace creates typespace/namespace/workspace workspace
	// with the key as wsKey. There are no checks to see if the workspace
	// already exists. To be used from only inside init functions of clients of
	// cqlWorkspaceDB.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	CreateWorkspace(c ctx, typespace string, namespace string,
		workspace string, nonce quantumfs.WorkspaceNonce,
		wsKey ObjectKey) error

	// BranchWorkspace branches srcTypespace/srcNamespace/srcWorkspace to create
	// dstTypespace/dstNamespace/dstWorkspace.
	// srcNonce and dstNonce is returned so it can inserted in the cache.
	//
	// Possible errors are:
	//  ErrWorkspaceExists
	//  ErrWorkspaceNotFound
	BranchWorkspace(c ctx, srcTypespace string, srcNamespace string,
		srcWorkspace string, dstTypespace string,
		dstNamespace string, dstWorkspace string) (quantumfs.WorkspaceNonce,
		quantumfs.WorkspaceNonce, error)

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
		nonce quantumfs.WorkspaceNonce, currentRootID ObjectKey,
		newRootID ObjectKey) (ObjectKey, quantumfs.WorkspaceNonce, error)

	SetWorkspaceImmutable(c ctx, typespace string, namespace string,
		workspace string) error

	WorkspaceIsImmutable(c ctx, typespace string, namespace string,
		workspace string) (bool, error)
}
