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
		state: make(map[string]map[string]map[string]bool),
	}

	// Create the null workspace
	type_ := quantumfs.NullTypespaceName
	name_ := quantumfs.NullNamespaceName
	work_ := quantumfs.NullWorkspaceName
	wsdb.cache[type_] = make(map[string]map[string]quantumfs.ObjectKey)
	wsdb.cache[type_][name_] = make(map[string]quantumfs.ObjectKey)
	wsdb.cache[type_][name_][work_] = quantumfs.EmptyWorkspaceKey

	wsdb.state[type_] = make(map[string]map[string]bool)
	wsdb.state[type_][name_] = make(map[string]bool)
	wsdb.state[type_][name_][work_] = true
	return wsdb
}

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex sync.RWMutex
	cache      map[string]map[string]map[string]quantumfs.ObjectKey
	stateMutex sync.RWMutex
	state      map[string]map[string]map[string]bool
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

func deleteWorkspace(c *quantumfs.Ctx, mutex sync.RWMutex,
	cache map[string]map[string]map[string]quantumfs.ObjectKey,
	typespace string, namespace string, workspace string) error {

	mutex.Lock()
	defer mutex.Unlock()

	_, ok := cache[typespace]
	if !ok {
		c.Vlog(qlog.LogWorkspaceDb, "typespace %s not found, success",
			typespace)
		return nil
	}

	_, ok = cache[typespace][namespace]
	if !ok {
		c.Vlog(qlog.LogWorkspaceDb, "namespace %s not found, success",
			namespace)
		return nil
	}

	c.Vlog(qlog.LogWorkspaceDb, "Deleting workspace %s", workspace)
	delete(cache[typespace][namespace], workspace)

	if len(cache[typespace][namespace]) == 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Deleting namespace %s", namespace)
		delete(cache[typespace], namespace)
	}

	if len(cache[typespace]) == 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Deleting typespace %s", typespace)
		delete(cache, typespace)
	}

	return nil
}

func deleteWorkspaceState(c *quantumfs.Ctx, mutex sync.RWMutex,
	cache map[string]map[string]map[string]bool,
	typespace string, namespace string, workspace string) error {

	mutex.Lock()
	defer mutex.Unlock()

	_, ok := cache[typespace]
	if !ok {
		c.Vlog(qlog.LogWorkspaceDb, "typespace %s not found, success",
			typespace)
		return nil
	}

	_, ok = cache[typespace][namespace]
	if !ok {
		c.Vlog(qlog.LogWorkspaceDb, "namespace %s not found, success",
			namespace)
		return nil
	}

	c.Vlog(qlog.LogWorkspaceDb, "Deleting workspace %s", workspace)
	delete(cache[typespace][namespace], workspace)

	if len(cache[typespace][namespace]) == 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Deleting namespace %s", namespace)
		delete(cache[typespace], namespace)
	}

	if len(cache[typespace]) == 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Deleting typespace %s", typespace)
		delete(cache, typespace)
	}

	return nil
}

func (wsdb *WorkspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	c.Vlog(qlog.LogWorkspaceDb, "processlocal::DeleteWorkspace %s/%s/%s",
		typespace, namespace, workspace)

	// Through all these checks, if the workspace could not exist, we return
	// success. The caller wanted that workspace to not exist and it doesn't.
	err := deleteWorkspace(c, wsdb.cacheMutex, wsdb.cache,
		typespace, namespace, workspace)
	if err != nil {
		return err
	}

	return deleteWorkspaceState(c, wsdb.stateMutex, wsdb.state,
		typespace, namespace, workspace)
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.RLock()
	rootid, exists := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	if !exists {
		return rootid, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such workspace")
	}
	return rootid, nil
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	wsdb.cacheMutex.Lock()
	rootId, exists := wsdb.workspace(c, typespace, namespace, workspace)
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

	wsdb.cache[typespace][namespace][workspace] = newRootId

	wsdb.cacheMutex.Unlock()

	c.Vlog(qlog.LogWorkspaceDb, "Advanced rootID for %s/%s from %s to %s",
		namespace, workspace, currentRootId.String(), newRootId.String())

	return newRootId, nil
}

func (wsdb *WorkspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	wsdb.stateMutex.RLock()
	defer wsdb.stateMutex.RUnlock()
	if _, exists := wsdb.state[typespace]; !exists {
		return exists, nil
	}

	if _, exists := wsdb.state[typespace][namespace]; !exists {
		return exists, nil
	}

	_, exists := wsdb.state[typespace][namespace][workspace]
	return exists, nil
}

func (wsdb *WorkspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	if exists, _ := wsdb.WorkspaceExists(c,
		typespace, namespace, workspace); !exists {

		return fmt.Errorf("Destination workspace doesn't exist")
	}

	wsdb.stateMutex.Lock()
	defer wsdb.stateMutex.Unlock()

	if _, exists := wsdb.state[typespace]; !exists {
		wsdb.state[typespace] = make(map[string]map[string]bool)
	}

	if _, exists := wsdb.state[typespace][namespace]; !exists {
		wsdb.state[typespace][namespace] = make(map[string]bool)
	}

	if _, exists := wsdb.state[typespace][namespace][workspace]; !exists {
		wsdb.state[typespace][namespace][workspace] = true
	}

	return nil
}
