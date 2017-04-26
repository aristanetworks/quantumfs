// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

type workspaceMap map[string]map[string]map[string]interface{}

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache: make(workspaceMap),
		state: make(workspaceMap),
	}

	type_ := quantumfs.NullSpaceName
	name_ := quantumfs.NullSpaceName
	work_ := quantumfs.NullSpaceName

	// Create the null workspace
	insertMap_(wsdb.cache, type_, name_, work_, quantumfs.EmptyWorkspaceKey)
	insertMap_(wsdb.state, type_, name_, work_, true)

	return wsdb
}

// The function requires the mutex on the map except for the NewWorkspaceDB
func insertMap_(cache workspaceMap, typespace string,
	namespace string, workspace string, val interface{}) error {

	if _, exists := cache[typespace]; !exists {
		cache[typespace] = make(map[string]map[string]interface{})
	}

	if _, exists := cache[typespace][namespace]; !exists {
		cache[typespace][namespace] = make(map[string]interface{})
	}

	if _, exists := cache[typespace][namespace][workspace]; exists {
		return fmt.Errorf("Destination Workspace already exists")
	}

	cache[typespace][namespace][workspace] = val
	return nil

}

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex sync.RWMutex
	cache      workspaceMap
	stateMutex sync.RWMutex
	state      workspaceMap
}

func (wsdb *WorkspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::NumTypespaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::NumTypespaces")

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::TypespaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::TypespaceList")

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

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::NumNamespaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::NumNamespaces")

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::NamespaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::NamespaceList")

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

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::NumWorkspaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::NumWorkspaces")

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace][namespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

// Assume WorkspaceExists run prior to this function everytime when it is called
// Otherwise, it probably tries to fetch non-existing key-value pairs
func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) ([]string, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::WorkspaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::WorkspaceList")

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

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::TypespaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::TypespaceExists")

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.cache[typespace]
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) namespace(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]interface{}, bool) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::namespace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::namespace")

	var workspacelist map[string]interface{}
	namespacelist, exists := wsdb.cache[typespace]
	if exists {
		workspacelist, exists = namespacelist[namespace]
	}

	return workspacelist, exists
}

func (wsdb *WorkspaceDB) workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, bool) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::workspace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::workspace")

	var rootId quantumfs.ObjectKey
	workspacelist, exists := wsdb.namespace(c, typespace, namespace)
	if !exists {
		return rootId, exists
	}

	rootVar, exists := workspacelist[workspace]
	if !exists {
		return rootId, exists
	}

	rootId, _ = rootVar.(quantumfs.ObjectKey)
	return rootId, exists
}

func (wsdb *WorkspaceDB) NamespaceExists(c *quantumfs.Ctx, typespace string,
	namespace string) (bool, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::NamespaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::NamespaceExists")

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.namespace(c, typespace, namespace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) WorkspaceExists(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::WorkspaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::WorkspaceExists")

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::BranchWorkspace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::BranchWorkspace")

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()

	if _, exists := wsdb.workspace(c, srcTypespace,
		srcNamespace, srcWorkspace); !exists {

		return fmt.Errorf("Source Workspace doesn't exist")
	}

	insertMap_(wsdb.cache, dstTypespace, dstNamespace, dstWorkspace,
		wsdb.cache[srcTypespace][srcNamespace][srcWorkspace])

	key := wsdb.cache[dstTypespace][dstNamespace][dstWorkspace]
	keyDebug := key.(quantumfs.ObjectKey).String()

	c.Dlog(qlog.LogWorkspaceDb,
		"Branched workspace '%s/%s/%s' to '%s/%s/%s' with key %s",
		srcTypespace, srcNamespace, srcWorkspace, dstTypespace,
		dstNamespace, dstWorkspace, keyDebug)

	return nil
}

// The given cache must be locked by its corresponding mutex
func deleteWorkspaceRecord_(c *quantumfs.Ctx, cache workspaceMap,
	typespace string, namespace string, workspace string) error {

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

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::DeleteWorkspace %s/%s/%s",
		typespace, namespace, workspace)
	defer c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::DeleteWorkspace")

	// Through all these checks, if the workspace could not exist, we return
	// success. The caller wanted that workspace to not exist and it doesn't.
	err := func() error {
		wsdb.cacheMutex.Lock()
		defer wsdb.cacheMutex.Unlock()
		return deleteWorkspaceRecord_(c, wsdb.cache, typespace,
			namespace, workspace)
	}()
	if err != nil {
		return err
	}

	wsdb.stateMutex.Lock()
	defer wsdb.stateMutex.Unlock()
	return deleteWorkspaceRecord_(c, wsdb.state, typespace, namespace, workspace)
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::Workspace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::Workspace")

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

	c.Vlog(qlog.LogWorkspaceDb, "---In processlocal::AdvanceWorkspace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- processlocal::AdvanceWorkspace")

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()
	rootId, exists := wsdb.workspace(c, typespace, namespace, workspace)
	if !exists {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Advance failed")
		return rootId, e
	}

	if currentRootId != rootId {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
			"%s vs %s Advance failed.", currentRootId.String(),
			rootId.String())
		return rootId, e
	}

	wsdb.cache[typespace][namespace][workspace] = newRootId

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
	insertMap_(wsdb.state, typespace, namespace, workspace, true)

	return nil
}
