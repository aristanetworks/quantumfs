// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// The gRPC WorkspaceDB backend is a networked stub which communicates with an RPC
// server which stores the actual data and performs the real logic. The only real
// logic this backend provides is disconnection detection and recovery, which
// involves re-subscribing for updates and ensuring the most recent state has been
// processed.

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	wsdb := &workspaceDB{
		config:        conf,
		subscriptions: map[string]bool{},
	}

	wsdb.reconnect()

	return wsdb
}

type workspaceDB struct {
	config        string
	lock          utils.DeferableMutex
	server        rpc.WorkspaceDbClient
	callback      quantumfs.SubscriptionCallback
	subscriptions map[string]bool
}

func (wsdb *workspaceDB) reconnect() {
	connOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(100 * time.Millisecond),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Second,
			Timeout:             1 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	var conn *grpc.ClientConn

	err := fmt.Errorf("Not an error")
	for err != nil {
		conn, err = grpc.Dial(wsdb.config, connOptions...)
	}

	wsdb.server = rpc.NewWorkspaceDbClient(conn)
}

func (wsdb *workspaceDB) handleGrpcError(err error) error {
	// TODO Reconnect
	return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_FATAL_DB_ERROR,
		"gRPC failed: %s", err.Error())
}

func (wsdb *workspaceDB) convertErr(c *quantumfs.Ctx, response rpc.Response) error {
	return quantumfs.NewWorkspaceDbErr(quantumfs.WsdbErrCode(response.Err),
		response.ErrCause)
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumTypespaces").Out()

	request := rpc.RequestId{Id: c.RequestId}

	response, err := wsdb.server.NumTypespaces(context.TODO(), &request)
	if err != nil {
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return 0, wsdb.convertErr(c, *response.Header)
	}

	return int(response.NumTypespaces), nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::TypespaceList").Out()

	request := rpc.RequestId{Id: c.RequestId}

	response, err := wsdb.server.TypespaceTable(context.TODO(), &request)
	if err != nil {
		return []string{}, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return []string{}, wsdb.convertErr(c, *response.Header)
	}

	return response.Typespaces, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumNamespaces").Out()

	request := rpc.NamespaceRequest{
		Header:    &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	response, err := wsdb.server.NumNamespaces(context.TODO(), &request)
	if err != nil {
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return 0, wsdb.convertErr(c, *response.Header)
	}

	return int(response.NumNamespaces), nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NamespaceList").Out()

	request := rpc.NamespaceRequest{
		Header:    &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	response, err := wsdb.server.NamespaceTable(context.TODO(), &request)
	if err != nil {
		return []string{}, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return []string{}, wsdb.convertErr(c, *response.Header)
	}

	return response.Namespaces, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumWorkspaces").Out()

	request := rpc.WorkspaceRequest{
		Header:    &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	response, err := wsdb.server.NumWorkspaces(context.TODO(), &request)
	if err != nil {
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return 0, wsdb.convertErr(c, *response.Header)
	}

	return int(response.NumWorkspaces), nil
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::WorkspaceList").Out()

	request := rpc.WorkspaceRequest{
		Header:    &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	workspaces := map[string]quantumfs.WorkspaceNonce{}

	response, err := wsdb.server.WorkspaceTable(context.TODO(), &request)
	if err != nil {
		return workspaces, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		return workspaces, wsdb.convertErr(c, *response.Header)
	}

	for name, nonce := range response.Workspaces {
		workspaces[name] = quantumfs.WorkspaceNonce(nonce.Nonce)
	}

	return workspaces, nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::BranchWorkspace").Out()

	request := rpc.BranchWorkspaceRequest{
		Header:      &rpc.RequestId{Id: c.RequestId},
		Source:      srcTypespace + "/" + srcNamespace + "/" + srcWorkspace,
		Destination: dstTypespace + "/" + dstNamespace + "/" + dstWorkspace,
	}

	response, err := wsdb.server.BranchWorkspace(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		return wsdb.convertErr(c, *response)
	}

	return nil
}

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::DeleteWorkspace").Out()

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      typespace + "/" + namespace + "/" + workspace,
	}

	response, err := wsdb.server.DeleteWorkspace(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		return wsdb.convertErr(c, *response)
	}

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

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      typespace + "/" + namespace + "/" + workspace,
	}

	response, err := wsdb.server.SetWorkspaceImmutable(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		return wsdb.convertErr(c, *response)
	}

	return nil
}

func (wsdb *workspaceDB) SetCallback(callback quantumfs.SubscriptionCallback) {
	defer wsdb.lock.Lock().Unlock()
	wsdb.callback = callback
}

func (wsdb *workspaceDB) SubscribeTo(workspaceName string) error {
	defer wsdb.lock.Lock().Unlock()
	wsdb.subscriptions[workspaceName] = true

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: 0},
		Name:      workspaceName,
	}

	response, err := wsdb.server.SubscribeTo(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		return wsdb.convertErr(c, *response)
	}

	return nil
}

func (wsdb *workspaceDB) UnsubscribeFrom(workspaceName string) {
	defer wsdb.lock.Lock().Unlock()
	delete(wsdb.subscriptions, workspaceName)

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: 0},
		Name:      workspaceName,
	}

	response, err := wsdb.server.UnsubscribeFrom(context.TODO(), &request)
	if err != nil {
		wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		err := wsdb.convertErr(c, *response)
		panic(fmt.Sprintf("Unexpected error from server: %s", err.Error()))
	}
}
