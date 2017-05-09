// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

type workspaceMap map[string]map[string]map[string]interface{}

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &workspaceDB{
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

// workspaceDB is a process local quantumfs.WorkspaceDB
type workspaceDB struct {
	cacheMutex sync.RWMutex
	cache      workspaceMap
	stateMutex sync.RWMutex
	state      workspaceMap
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NumTypespaces").Out()

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::TypespaceList").Out()

	wsdb.cacheMutex.RLock()
	typespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		typespaces = append(typespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return typespaces, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::NumNamespaces").Out()

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NamespaceList").Out()

	wsdb.cacheMutex.RLock()
	namespaces := make([]string, 0, len(wsdb.cache[typespace]))

	for name, _ := range wsdb.cache[typespace] {
		namespaces = append(namespaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return namespaces, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NumWorkspaces").Out()

	wsdb.cacheMutex.RLock()
	num := len(wsdb.cache[typespace][namespace])
	wsdb.cacheMutex.RUnlock()

	return num, nil
}

// Assume WorkspaceExists run prior to this function everytime when it is called
// Otherwise, it probably tries to fetch non-existing key-value pairs
func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::WorkspaceList").Out()

	wsdb.cacheMutex.RLock()
	workspaces := make([]string, 0, len(wsdb.cache[typespace][namespace]))

	for name, _ := range wsdb.cache[typespace][namespace] {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.RUnlock()

	return workspaces, nil
}

func (wsdb *workspaceDB) TypespaceExists(c *quantumfs.Ctx, typespace string) (bool,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::TypespaceExists").Out()

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.cache[typespace]
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *workspaceDB) namespace(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]interface{}, bool) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::namespace").Out()

	var workspacelist map[string]interface{}
	namespacelist, exists := wsdb.cache[typespace]
	if exists {
		workspacelist, exists = namespacelist[namespace]
	}

	return workspacelist, exists
}

func (wsdb *workspaceDB) workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, bool) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::workspace").Out()

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

func (wsdb *workspaceDB) NamespaceExists(c *quantumfs.Ctx, typespace string,
	namespace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NamespaceExists").Out()

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.namespace(c, typespace, namespace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *workspaceDB) WorkspaceExists(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::WorkspaceExists").Out()

	wsdb.cacheMutex.RLock()
	_, exists := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	return exists, nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::BranchWorkspace").Out()

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

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn(qlog.LogWorkspaceDb, "processlocal::DeleteWorkspace %s/%s/%s",
		typespace, namespace, workspace).Out()
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

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::Workspace").Out()

	wsdb.cacheMutex.RLock()
	rootid, exists := wsdb.workspace(c, typespace, namespace, workspace)
	wsdb.cacheMutex.RUnlock()

	if !exists {
		return rootid, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such workspace")
	}
	return rootid, nil
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::AdvanceWorkspace").Out()

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()
	rootId, exists := wsdb.workspace(c, typespace, namespace, workspace)
	if !exists {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Advance failed")
		return rootId, e
	}

	if !currentRootId.IsEqualTo(rootId) {
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

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
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

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
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
