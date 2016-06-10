// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "arista.com/quantumfs"

func NewWorkspaceDB() quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache: make(map[string]map[string]quantumfs.ObjectKey),
	}

	// Create the null workspace
	wsdb.cache[quantumfs.NullNamespaceName] =
		make(map[string]quantumfs.ObjectKey)
	wsdb.cache[quantumfs.NullNamespaceName][quantumfs.NullWorkspaceName] =
		quantumfs.EmptyWorkspaceKey
	return wsdb
}

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex sync.RWMutex
	cache      map[string]map[string]quantumfs.ObjectKey
}

func (wsdb *WorkspaceDB) NumNamespaces() int {
	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.RUnlock()

	return num
}

func (wsdb *WorkspaceDB) NamespaceList() []string {
	wsdb.cacheMutex.RLock()
	namespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		namespaces = append(namespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return namespaces
}

func (wsdb *WorkspaceDB) NumWorkspaces(namespace string) int {
	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[namespace])
	wsdb.cacheMutex.RUnlock()

	return num
}

func (wsdb *WorkspaceDB) WorkspaceList(namespace string) []string {
	wsdb.cacheMutex.RLock()
	workspaces := make([]string, 0, len(wsdb.cache[namespace]))

	for name, _ := range wsdb.cache[namespace] {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return workspaces
}

func (wsdb *WorkspaceDB) NamespaceExists(namespace string) bool {
	wsdb.cacheMutex.RLock()
	_, exists := wsdb.cache[namespace]
	wsdb.cacheMutex.RUnlock()

	return exists
}

// Non-lock grabbing variant of workspace
func (wsdb *WorkspaceDB) workspace(namespace string, workspace string) (
	quantumfs.ObjectKey, bool) {
	var rootid quantumfs.ObjectKey
	workspacelist, exists := wsdb.cache[namespace]
	if exists {
		rootid, exists = workspacelist[workspace]
	}

	return rootid, exists
}

func (wsdb *WorkspaceDB) WorkspaceExists(namespace string, workspace string) bool {
	wsdb.cacheMutex.RLock()
	_, exists := wsdb.workspace(namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return exists
}

func (wsdb *WorkspaceDB) BranchWorkspace(srcNamespace string, srcWorkspace string,
	dstNamespace string, dstWorkspace string) error {

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()

	if _, exists := wsdb.workspace(srcNamespace, srcWorkspace); !exists {
		return fmt.Errorf("Source Workspace doesn't exist")
	}

	if _, exists := wsdb.cache[dstNamespace]; !exists {
		wsdb.cache[dstNamespace] = make(map[string]quantumfs.ObjectKey)
	} else if _, exists := wsdb.cache[dstNamespace][dstWorkspace]; exists {
		return fmt.Errorf("Destination Workspace already exists")
	}

	wsdb.cache[dstNamespace][dstWorkspace] =
		wsdb.cache[srcNamespace][srcWorkspace]

	return nil
}

func (wsdb *WorkspaceDB) Workspace(namespace string,
	workspace string) quantumfs.ObjectKey {

	wsdb.cacheMutex.RLock()
	rootid, _ := wsdb.workspace(namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return rootid
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(namespace string, workspace string,
	currentRootId quantumfs.ObjectKey, newRootId quantumfs.ObjectKey) (
	quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.Lock()
	rootId, exists := wsdb.workspace(namespace, workspace)
	if !exists {
		wsdb.cacheMutex.Unlock()
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND)
		return rootId, e
	}

	if currentRootId != rootId {
		wsdb.cacheMutex.Unlock()
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE)
		return rootId, e
	}

	wsdb.cache[namespace][workspace] = newRootId

	wsdb.cacheMutex.Unlock()

	return newRootId, nil
}
