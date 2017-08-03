// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type workspaceInfo struct {
	key       quantumfs.ObjectKey
	nonce     quantumfs.WorkspaceNonce
	immutable bool
}

type workspaceMap map[string]map[string]map[string]*workspaceInfo

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache:         make(workspaceMap),
		cacheMutex:    new(utils.DeferableRwMutex),
		callback:      nil,
		updates:       nil,
		subscriptions: map[string]bool{},
	}

	type_ := quantumfs.NullSpaceName
	name_ := quantumfs.NullSpaceName
	work_ := quantumfs.NullSpaceName

	// Create the null workspace
	nullWorkspace := workspaceInfo{
		key:       quantumfs.EmptyWorkspaceKey,
		nonce:     0,
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

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex *utils.DeferableRwMutex
	cache      workspaceMap

	callback      quantumfs.SubscriptionCallback
	updates       map[string]quantumfs.WorkspaceState
	subscriptions map[string]bool

	peer *WorkspaceDB
}

func (wsdb *WorkspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::NumTypespaces").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	num := len(wsdb.cache)

	return num, nil
}

func (wsdb *WorkspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::TypespaceList").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	typespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		typespaces = append(typespaces, name)
	}

	return typespaces, nil
}

func (wsdb *WorkspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "processlocal::NumNamespaces").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	namespaces, err := wsdb.typespace_(c, typespace)
	if err != nil {
		return 0, err
	}

	return len(namespaces), nil
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
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

func (wsdb *WorkspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
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
func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::WorkspaceList").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	workspaces, err := wsdb.namespace_(c, typespace, namespace)
	if err != nil {
		return nil, err
	}

	workspaceList := make(map[string]quantumfs.WorkspaceNonce, len(workspaces))

	for name, info := range workspaces {
		workspaceList[name] = info.nonce
	}

	return workspaceList, nil
}

// Must hold cacheMutex for read
func (wsdb *WorkspaceDB) typespace_(c *quantumfs.Ctx,
	typespace string) (map[string]map[string]*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::typespace_").Out()

	namespaces, exists := wsdb.cache[typespace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such typespace")
	}

	return namespaces, nil
}

// Must hold cacheMutex for read
func (wsdb *WorkspaceDB) namespace_(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::namespace_").Out()

	namespaces, err := wsdb.typespace_(c, typespace)
	if err != nil {
		return nil, err
	}
	workspaces, exists := namespaces[namespace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such namespace")
	}

	return workspaces, nil
}

// Must hold cacheMutex for read
func (wsdb *WorkspaceDB) workspace_(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (*workspaceInfo, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::workspace_").Out()

	workspaces, err := wsdb.namespace_(c, typespace, namespace)
	if err != nil {
		return nil, err
	}

	info, exists := workspaces[workspace]
	if !exists {
		return nil, quantumfs.NewWorkspaceDbErr(
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "No such workspace")
	}

	return info, nil
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
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
		nonce:     quantumfs.WorkspaceNonce(time.Now().UnixNano()),
		immutable: false,
	}
	insertMap_(wsdb.cache, dstTypespace, dstNamespace, dstWorkspace, &newInfo)

	wsdb.notifySubscribers_(c, dstTypespace, dstNamespace, dstWorkspace, true)

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

func (wsdb *WorkspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn(qlog.LogWorkspaceDb, "processlocal::DeleteWorkspace %s/%s/%s",
		typespace, namespace, workspace).Out()
	// Through all these checks, if the workspace could not exist, we return
	// success. The caller wanted that workspace to not exist and it doesn't.
	defer wsdb.cacheMutex.Lock().Unlock()
	err := deleteWorkspaceRecord_(c, wsdb.cache, typespace, namespace, workspace)

	wsdb.notifySubscribers_(c, typespace, namespace, workspace, true)

	return err
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::Workspace").Out()

	defer wsdb.cacheMutex.RLock().RUnlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, 0, err
	}
	return info.key, info.nonce, nil
}

func (wsdb *WorkspaceDB) FetchAndSubscribeWorkspace(c *quantumfs.Ctx,
	typespace string, namespace string, workspace string) (
	quantumfs.ObjectKey, quantumfs.WorkspaceNonce, error) {

	err := wsdb.SubscribeTo(typespace + "/" + namespace + "/" + workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, 0, err
	}

	return wsdb.Workspace(c, typespace, namespace, workspace)
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, nonce quantumfs.WorkspaceNonce,
	currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::AdvanceWorkspace").Out()

	defer wsdb.cacheMutex.Lock().Unlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		wsdbErr := err.(*quantumfs.WorkspaceDbErr)
		e := quantumfs.NewWorkspaceDbErr(wsdbErr.Code, "Advance failed: %s",
			wsdbErr.ErrorCode())
		return quantumfs.ZeroKey, e
	}

	if nonce != info.nonce {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
			"Nonce %d does not match WSDB (%d)", nonce, info.nonce)
		return info.key, e
	}

	if !currentRootId.IsEqualTo(info.key) {
		e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
			"%s vs %s Advance failed.", currentRootId.String(),
			info.key.String())
		return info.key, e
	}

	wsdb.cache[typespace][namespace][workspace].key = newRootId

	wsdb.notifySubscribers_(c, typespace, namespace, workspace, true)

	c.Vlog(qlog.LogWorkspaceDb, "Advanced rootID for %s/%s from %s to %s",
		namespace, workspace, currentRootId.String(), newRootId.String())

	return newRootId, nil
}

func (wsdb *WorkspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer wsdb.cacheMutex.RLock().RUnlock()
	info, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return false, err
	}

	return info.immutable, nil
}

func (wsdb *WorkspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer wsdb.cacheMutex.Lock().Unlock()
	workspaceInfo, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		return err
	}

	workspaceInfo.immutable = true

	wsdb.notifySubscribers_(c, typespace, namespace, workspace, true)

	return nil
}

func (wsdb *WorkspaceDB) SetCallback(callback quantumfs.SubscriptionCallback) {
	defer wsdb.cacheMutex.Lock().Unlock()
	wsdb.callback = callback
}

func (wsdb *WorkspaceDB) SubscribeTo(workspaceName string) error {
	defer wsdb.cacheMutex.Lock().Unlock()
	wsdb.subscriptions[workspaceName] = true

	return nil
}

func (wsdb *WorkspaceDB) UnsubscribeFrom(workspaceName string) {
	defer wsdb.cacheMutex.Lock().Unlock()
	delete(wsdb.subscriptions, workspaceName)
}

// Must hold cacheMutex
func (wsdb *WorkspaceDB) notifySubscribers_(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, recurse bool) {

	c.FuncIn(qlog.LogWorkspaceDb, "processlocal::notifySubscribers_",
		"workspace %s/%s/%s", typespace, namespace, workspace)

	workspaceName := typespace + "/" + namespace + "/" + workspace

	if wsdb.peer != nil && recurse {
		wsdb.peer.notifySubscribers_(c, typespace, namespace, workspace,
			false)
	}

	if _, subscribed := wsdb.subscriptions[workspaceName]; !subscribed {
		c.Vlog(qlog.LogWorkspaceDb, "No subscriptions for workspace")
		return
	}

	startTransmission := false
	if wsdb.updates == nil {
		c.Vlog(qlog.LogWorkspaceDb, "No notification goroutine running")
		wsdb.updates = map[string]quantumfs.WorkspaceState{}
		startTransmission = true
	}

	// Update/set the state of this workspace
	state := quantumfs.WorkspaceState{}

	wsInfo, err := wsdb.workspace_(c, typespace, namespace, workspace)
	if err != nil {
		switch err := err.(type) {
		default:
			c.Elog(qlog.LogWorkspaceDb,
				"Unknown error type fetching workspace: %s",
				err.Error())
			return
		case *quantumfs.WorkspaceDbErr:
			switch err.Code {
			default:
				c.Elog(qlog.LogWorkspaceDb,
					"Unexpected error fetching workspace: %s",
					err.Error())
				return
			case quantumfs.WSDB_WORKSPACE_NOT_FOUND:
				c.Vlog(qlog.LogWorkspaceDb, "Workspace was deleted")
				state.Deleted = true
			}
		}
	} else {
		// If we didn't receive an error, then we have valid information to
		// pass to the client.
		state.RootId = wsInfo.key
		state.Nonce = wsInfo.nonce
		state.Immutable = wsInfo.immutable
	}

	wsdb.updates[workspaceName] = state

	// Possibly start notifying the client.
	if !startTransmission {
		// There is already an update in progress and we need to wait for
		// that to complete. The goroutine which is running the callback will
		// find these new updates and send them when it completes.
		return
	}

	go wsdb.sendNotifications(c)
}

// Send all notifications to the registered callback. This should be run in its own
// goroutine as it will repeatedly run the callback on any notifications which arrive
// while the callback is processing the previous set of updates.
func (wsdb *WorkspaceDB) sendNotifications(c *quantumfs.Ctx) {
	defer c.FuncInName(qlog.LogWorkspaceDb,
		"processlocal::sendNotifications").Out()

	var callback quantumfs.SubscriptionCallback
	var updates map[string]quantumfs.WorkspaceState

	for {
		func() {
			c.Vlog(qlog.LogWorkspaceDb, "Checking for updates")
			defer wsdb.cacheMutex.Lock().Unlock()
			callback = wsdb.callback
			updates = wsdb.updates

			if len(updates) == 0 {
				// No new updates since the last time around, we have
				// caught up.
				wsdb.updates = nil
				updates = nil
			} else {
				// There have been new updates since the previous
				// time through the loop. Loop again.
				wsdb.updates = map[string]quantumfs.WorkspaceState{}
			}
		}()

		if updates == nil {
			c.Vlog(qlog.LogWorkspaceDb, "No further updates")
			return
		}

		if callback != nil {
			c.Vlog(qlog.LogWorkspaceDb,
				"Notifying callback of %d updates", len(updates))
			quantumfs.SafelyCallSubscriptionCallback(callback, updates)
		} else {
			c.Vlog(qlog.LogWorkspaceDb, "nil callback, dropping %d "+
				"updates", len(updates))
		}
	}
}

func (wsdb *WorkspaceDB) GetSecondHead() *WorkspaceDB {
	wsdb2 := NewWorkspaceDB("").(*WorkspaceDB)
	wsdb2.cache = wsdb.cache
	wsdb2.cacheMutex = wsdb.cacheMutex
	wsdb2.peer = wsdb
	wsdb.peer = wsdb2

	return wsdb2
}
