// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

// The gRPC WorkspaceDB backend is a networked stub which communicates with an RPC
// server which stores the actual data and performs the real logic. The only real
// logic this backend provides is disconnection detection and recovery, which
// involves re-subscribing for updates and ensuring the most recent state has been
// processed.

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &workspaceDB{}
	return wsdb
}

type workspaceDB struct {
	lock          utils.DeferableMutex
	callback      quantumfs.SubscriptionCallback
	subscriptions map[string]bool
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	var num int

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumTypespaces").Out()

	return num, nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	typespaceList := make([]string, 0, 100)

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::TypespaceList").Out()

	return typespaceList, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumNamespaces").Out()

	var num int

	return num, nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NamespaceList").Out()

	namespaceList := make([]string, 0, 100)

	return namespaceList, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumWorkspaces").Out()

	var num int

	return num, nil
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::WorkspaceList").Out()

	workspaceList := make(map[string]quantumfs.WorkspaceNonce, 100)

	return workspaceList, nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::BranchWorkspace").Out()

	return nil
}

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::DeleteWorkspace").Out()

	return nil
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::Workspace").Out()

	var nonce quantumfs.WorkspaceNonce

	return quantumfs.ObjectKey{}, nonce, nil
}

func (wsdb *workspaceDB) FetchAndSubscribeWorkspace(c *quantumfs.Ctx,
	typespace string, namespace string, workspace string) (
	quantumfs.ObjectKey, quantumfs.WorkspaceNonce, error) {

	err := wsdb.SubscribeTo(typespace + "/" + namespace + "/" + workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, 0, err
	}

	return wsdb.Workspace(c, typespace, namespace, workspace)
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, nonce quantumfs.WorkspaceNonce,
	currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::AdvanceWorkspace").Out()

	return quantumfs.ObjectKey{}, nil
}

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	var immutable bool

	return immutable, nil
}

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	return nil
}

func (wsdb *workspaceDB) SetCallback(callback quantumfs.SubscriptionCallback) {
	defer wsdb.lock.Lock().Unlock()
	wsdb.callback = callback
}

func (wsdb *workspaceDB) SubscribeTo(workspaceName string) error {
	defer wsdb.lock.Lock().Unlock()
	wsdb.subscriptions[workspaceName] = true

	return nil
}

func (wsdb *workspaceDB) UnsubscribeFrom(workspaceName string) {
	defer wsdb.lock.Lock().Unlock()
	delete(wsdb.subscriptions, workspaceName)
}
