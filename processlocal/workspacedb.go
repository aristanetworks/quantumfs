// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type workspaceInfo struct {
	key       quantumfs.ObjectKey
	immutable bool
}

type workspaceMap map[string]map[string]map[string]*workspaceInfo

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &workspaceDB{
		cache: make(workspaceMap),
	}

	type_ := quantumfs.NullSpaceName
	name_ := quantumfs.NullSpaceName
	work_ := quantumfs.NullSpaceName

	// Create the null workspace
	nullWorkspace := workspaceInfo{
		key:       quantumfs.EmptyWorkspaceKey,
		immutable: true,
	}
	insertMap_(wsdb.cache, type_, name_, work_, &nullWorkspace)

	return wsdb
}

// The function requires the mutex on the map except for the NewWorkspaceDB
func insertMap_(cache workspaceMap, typespace string,
	namespace string, workspace string, info *workspaceInfo) error {

	if _, exists := cache[typespace]; !exists {
		cache[typespace] = make(map[string]map[string]*workspaceInfo)
	}

	if _, exists := cache[typespace][namespace]; !exists {
		cache[typespace][namespace] = make(map[string]*workspaceInfo)
	}

	if _, exists := cache[typespace][namespace][workspace]; exists {
		return fmt.Errorf("Destination Workspace already exists")
	}

	cache[typespace][namespace][workspace] = info
	return nil

}

// workspaceDB is a process local quantumfs.WorkspaceDB
type workspaceDB struct {
	cacheMutex utils.DeferableRwMutex
	cache      workspaceMap
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NumTypespaces").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	num := len(wsdb.cache)

	return num, nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::TypespaceList").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	typespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		typespaces = append(typespaces, name)
	}

	return typespaces, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::NumNamespaces").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	namespaces, err := wsdb.typespace_(c, typespace)
	if err != nil {
		return 0, err
	}

	return len(namespaces), nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NamespaceList").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	namespaces, err := wsdb.typespace_(c, typespace)
	if err != nil {
		return nil, err
	}

	namespaceList := make([]string, 0, len(wsdb.cache[typespace]))

	for name, _ := range namespaces {
		namespaceList = append(namespaceList, name)
	}

	return namespaceList, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NumWorkspaces").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	workspaces, err := wsdb.namespace_(c, typespace, namespace)
	if err != nil {
		return 0, err
	}

	return len(workspaces), nil
}

// Assume WorkspaceExists run prior to this function everytime when it is called
// Otherwise, it probably tries to fetch non-existing key-value pairs
func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::WorkspaceList").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	workspaces, err := wsdb.namespace_(c, typespace, namespace)
	if err != nil {
		return nil, err
	}

	workspaceList := make([]string, 0, len(workspaces))

	for name, _ := range workspaces {
		workspaceList = append(workspaceList, name)
	}

	return workspaceList, nil
}

// Must hold cacheMutex for read
func (wsdb *workspaceDB) typespace_(c *quantumfs.Ctx,
	typespace string) (map[string]map[string]*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::typespace_").Out()

	namespacelist, exists := wsdb.cache[typespace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such typespace")
	}

	return namespacelist, nil
}

// Must hold cacheMutex for read
func (wsdb *workspaceDB) namespace_(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::namespace_").Out()

	namespacelist, err := wsdb.typespace_(c, typespace)
	if err != nil {
		return nil, err
	}
	workspacelist, exists := namespacelist[namespace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such namespace")
	}

	return workspacelist, nil
}

// Must hold cacheMutex for read
func (wsdb *workspaceDB) workspace_(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::workspace_").Out()

	workspacelist, err := wsdb.namespace_(c, typespace, namespace)
	if err != nil {
		return nil, err
	}

	info, exists := workspacelist[workspace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such workspace")
	}

	return info, nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::BranchWorkspace").Out()

	defer wsdb.cacheMutex.Lock().Unlock()

	info, err := wsdb.workspace_(c, srcTypespace, srcNamespace, srcWorkspace)
	if err != nil {
		return err
	}

	newInfo := workspaceInfo{
		key:       info.key,
		immutable: false,
	}
	insertMap_(wsdb.cache, dstTypespace, dstNamespace, dstWorkspace, &newInfo)

	keyDebug := newInfo.key.String()

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
	defer wsdb.cacheMutex.Lock().Unlock()
	return deleteWorkspaceRecord_(c, wsdb.cache, typespace, namespace, workspace)
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::Workspace").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}
	return info.key, nil
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::AdvanceWorkspace").Out()

	defer wsdb.cacheMutex.Lock().Unlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		wsdbErr := err.(quantumfs.WorkspaceDbErr)
		e := quantumfs.NewWorkspaceDbErr(wsdbErr.Code, "Advance failed: %s",
			wsdbErr.ErrorCode())
		return info.key, e
	}

	if !currentRootId.IsEqualTo(info.key) {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
			"%s vs %s Advance failed.", currentRootId.String(),
			info.key.String())
		return info.key, e
	}

	wsdb.cache[typespace][namespace][workspace].key = newRootId

	c.Vlog(qlog.LogWorkspaceDb, "Advanced rootID for %s/%s from %s to %s",
		namespace, workspace, currentRootId.String(), newRootId.String())

	return newRootId, nil
}

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer wsdb.cacheMutex.RLock().RUnlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return false, err
	}

	return info.immutable, nil
}

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer wsdb.cacheMutex.Lock().Unlock()
	workspaceInfo, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return err
	}

	workspaceInfo.immutable = true

	return nil
}
