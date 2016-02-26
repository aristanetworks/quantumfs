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
	NumNamespaces() int
	NamespaceList() []string
	NumWorkspaces(namespace string) int
	WorkspaceList(namespace string) []string

	// These methods need to be up to date
	NamespaceExists(namespace string) bool
	WorkspaceExists(namespace string, workspace string) bool
}
