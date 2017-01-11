// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
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

func (wsdb *WorkspaceDB) NumNamespaces(c *quantumfs.Ctx) (int, error) {
	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx) ([]string, error) {
	wsdb.cacheMutex.RLock()
	namespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		namespaces = append(namespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return namespaces, nil
}

func (wsdb *WorkspaceDB) NumWorkspaces(c *quantumfs.Ctx, namespace string) (int,
	error) {

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[namespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, namespace string) ([]string,
	error) {

	wsdb.cacheMutex.RLock()
	workspaces := make([]string, 0, len(wsdb.cache[namespace]))

	for name, _ := range wsdb.cache[namespace] {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return workspaces, nil
}

func (wsdb *WorkspaceDB) NamespaceExists(c *quantumfs.Ctx, namespace string) (bool,
	error) {

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.cache[namespace]
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

// Non-lock grabbing variant of workspace
func (wsdb *WorkspaceDB) workspace(c *quantumfs.Ctx, namespace string,
	workspace string) (quantumfs.ObjectKey, bool) {

	var rootid quantumfs.ObjectKey
	workspacelist, exists := wsdb.cache[namespace]
	if exists {
		rootid, exists = workspacelist[workspace]
	}

	return rootid, exists
}

func (wsdb *WorkspaceDB) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) (bool, error) {

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.workspace(c, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()

	if _, exists := wsdb.workspace(c, srcNamespace, srcWorkspace); !exists {
		return fmt.Errorf("Source Workspace doesn't exist")
	}

	if _, exists := wsdb.cache[dstNamespace]; !exists {
		wsdb.cache[dstNamespace] = make(map[string]quantumfs.ObjectKey)
	} else if _, exists := wsdb.cache[dstNamespace][dstWorkspace]; exists {
		return fmt.Errorf("Destination Workspace already exists")
	}

	wsdb.cache[dstNamespace][dstWorkspace] =
		wsdb.cache[srcNamespace][srcWorkspace]

	keyDebug := wsdb.cache[dstNamespace][dstWorkspace].String()

	c.Dlog(qlog.LogWorkspaceDb,
		"Branched workspace '%s/%s' to '%s/%s' with key %s", srcNamespace,
		srcWorkspace, dstNamespace, dstWorkspace,
		keyDebug)

	return nil
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.RLock()
	rootid, _ := wsdb.workspace(c, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return rootid, nil
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.Lock()
	rootId, exists := wsdb.workspace(c, namespace, workspace)
	if !exists {
		wsdb.cacheMutex.Unlock()
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Advance failed")
		return rootId, e
	}

	if currentRootId != rootId {
		wsdb.cacheMutex.Unlock()
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
			"%s vs %s Advance failed.", currentRootId.String(),
			rootId.String())
		return rootId, e
	}

	wsdb.cache[namespace][workspace] = newRootId

	wsdb.cacheMutex.Unlock()

	return newRootId, nil
}
