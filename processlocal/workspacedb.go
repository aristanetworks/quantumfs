// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache: make(map[string]map[string]map[string]quantumfs.ObjectKey),
	}

	// Create the null workspace
	type_ := quantumfs.NullTypespaceName
	name_ := quantumfs.NullNamespaceName
	work_ := quantumfs.NullWorkspaceName
	wsdb.cache[type_] = make(map[string]map[string]quantumfs.ObjectKey)
	wsdb.cache[type_][name_] = make(map[string]quantumfs.ObjectKey)
	wsdb.cache[type_][name_][work_] = quantumfs.EmptyWorkspaceKey

	return wsdb
}

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex sync.RWMutex
	cache      map[string]map[string]map[string]quantumfs.ObjectKey
}

func (wsdb *WorkspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	wsdb.cacheMutex.RLock()
	typespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		typespaces = append(typespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return typespaces, nil
}

func (wsdb *WorkspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	wsdb.cacheMutex.RLock()
	namespaces := make([]string, 0, len(wsdb.cache[typespace]))

	for name, _ := range wsdb.cache[typespace] {
		namespaces = append(namespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return namespaces, nil
}

func (wsdb *WorkspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace][namespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

// Assume WorkspaceExists run prior to this function everytime when it is called
// Otherwise, it probably tries to fetch non-existing key-value pairs
func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) ([]string, error) {

	wsdb.cacheMutex.RLock()
	workspaces := make([]string, 0, len(wsdb.cache[typespace][namespace]))

	for name, _ := range wsdb.cache[typespace][namespace] {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return workspaces, nil
}

func (wsdb *WorkspaceDB) TypespaceExists(c *quantumfs.Ctx, typespace string) (bool,
	error) {

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.cache[typespace]
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) namespace(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.ObjectKey, bool) {

	var workspacelist map[string]quantumfs.ObjectKey
	namespacelist, exists := wsdb.cache[typespace]
	if exists {
		workspacelist, exists = namespacelist[namespace]
	}

	return workspacelist, exists
}

func (wsdb *WorkspaceDB) workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, bool) {

	var rootId quantumfs.ObjectKey
	workspacelist, exists := wsdb.namespace(c, typespace, namespace)
	if exists {
		rootId, exists = workspacelist[workspace]
	}

	return rootId, exists
}

func (wsdb *WorkspaceDB) NamespaceExists(c *quantumfs.Ctx, typespace string,
	namespace string) (bool, error) {

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.namespace(c, typespace, namespace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) WorkspaceExists(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()

	if _, exists := wsdb.workspace(c, srcTypespace,
		srcNamespace, srcWorkspace); !exists {

		return fmt.Errorf("Source Workspace doesn't exist")
	}

	if _, exists := wsdb.cache[dstTypespace]; !exists {
		wsdb.cache[dstTypespace] =
			make(map[string]map[string]quantumfs.ObjectKey)
	}

	if _, exists := wsdb.cache[dstTypespace][dstNamespace]; !exists {
		wsdb.cache[dstTypespace][dstNamespace] =
			make(map[string]quantumfs.ObjectKey)
	}

	if _, exists :=
		wsdb.cache[dstTypespace][dstNamespace][dstWorkspace]; exists {

		return fmt.Errorf("Destination Workspace already exists")
	}

	wsdb.cache[dstTypespace][dstNamespace][dstWorkspace] =
		wsdb.cache[srcTypespace][srcNamespace][srcWorkspace]

	keyDebug := wsdb.cache[dstTypespace][dstNamespace][dstWorkspace].String()

	c.Dlog(qlog.LogWorkspaceDb,
		"Branched workspace '%s/%s/%s' to '%s/%s/%s' with key %s",
		srcTypespace, srcNamespace, srcWorkspace, dstTypespace,
		dstNamespace, dstWorkspace, keyDebug)

	return nil
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.RLock()
	rootid, _ := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return rootid, nil
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.Lock()
	rootId, exists := wsdb.workspace(c, typespace, namespace, workspace)
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

	wsdb.cache[typespace][namespace][workspace] = newRootId

	wsdb.cacheMutex.Unlock()

	return newRootId, nil
}
