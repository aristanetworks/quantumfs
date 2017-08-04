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
	updates       map[string]quantumfs.WorkspaceState
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

	go wsdb.waitForWorkspaceUpdates()
}

func (wsdb *workspaceDB) waitForWorkspaceUpdates() {
	stream, err := wsdb.server.ListenForUpdates(context.TODO(), &rpc.Void{})
	if err != nil {
		wsdb.reconnect()
		return
	}

	for {
		update, err := stream.Recv()
		if err != nil {
			wsdb.reconnect()
			return
		}

		startTransmission := false

		subscribed := func() bool {
			defer wsdb.lock.Lock().Unlock()
			if _, exists := wsdb.subscriptions[update.Name]; !exists {
				// We aren't subscribed for this workspace
				return false
			}

			if wsdb.updates == nil {
				startTransmission = true
			}
			wsdb.updates = map[string]quantumfs.WorkspaceState{}

			return true
		}()

		if !subscribed {
			return
		}

		wsdb.updates[update.Name] = quantumfs.WorkspaceState{
			RootId:    quantumfs.NewObjectKeyFromBytes(update.RootId.Data),
			Nonce:     quantumfs.WorkspaceNonce(update.Nonce),
			Immutable: update.Immutable,
			Deleted:   update.Deleted,
		}

		if !startTransmission {
			// There is already an update in progress and we need to wait for
			// that to complete. The goroutine which is running the callback will
			// find these new updates and send them when it completes.
			continue
		}

		go wsdb.sendNotifications()
	}

}

func (wsdb *workspaceDB) sendNotifications() {
	var callback quantumfs.SubscriptionCallback
	var updates map[string]quantumfs.WorkspaceState

	for {
		func() {
			defer wsdb.lock.Lock().Unlock()
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
			return
		}

		if callback != nil {
			quantumfs.SafelyCallSubscriptionCallback(callback, updates)
		}
	}
}

func (wsdb *workspaceDB) handleGrpcError(err error) error {
	// TODO Reconnect
	return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_FATAL_DB_ERROR,
		"gRPC failed: %s", err.Error())
}

func (wsdb *workspaceDB) convertErr(response rpc.Response) error {
	return quantumfs.NewWorkspaceDbErr(quantumfs.WsdbErrCode(response.Err),
		response.ErrCause)
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumTypespaces").Out()

	request := rpc.RequestId{Id: c.RequestId}

	response, err := wsdb.server.NumTypespaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumTypespaces), nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::TypespaceList").Out()

	request := rpc.RequestId{Id: c.RequestId}

	response, err := wsdb.server.TypespaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return []string{}, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return []string{}, wsdb.convertErr(*response.Header)
	}

	return response.Typespaces, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumNamespaces").Out()

	request := rpc.NamespaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	response, err := wsdb.server.NumNamespaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumNamespaces), nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NamespaceList").Out()

	request := rpc.NamespaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	response, err := wsdb.server.NamespaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return []string{}, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return []string{}, wsdb.convertErr(*response.Header)
	}

	return response.Namespaces, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::NumWorkspaces").Out()

	request := rpc.WorkspaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	response, err := wsdb.server.NumWorkspaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumWorkspaces), nil
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::WorkspaceList").Out()

	request := rpc.WorkspaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	workspaces := map[string]quantumfs.WorkspaceNonce{}

	response, err := wsdb.server.WorkspaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return workspaces, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return workspaces, wsdb.convertErr(*response.Header)
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
		RequestId:   &rpc.RequestId{Id: c.RequestId},
		Source:      srcTypespace + "/" + srcNamespace + "/" + srcWorkspace,
		Destination: dstTypespace + "/" + dstNamespace + "/" + dstWorkspace,
	}

	response, err := wsdb.server.BranchWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Err, response.ErrCause)
		return wsdb.convertErr(*response)
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
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Err, response.ErrCause)
		return wsdb.convertErr(*response)
	}

	return nil
}

func (wsdb *workspaceDB) fetchWorkspace(c *quantumfs.Ctx, workspaceName string) (
	key quantumfs.ObjectKey, nonce quantumfs.WorkspaceNonce, immutable bool,
	err error) {

	defer c.FuncIn(qlog.LogWorkspaceDb, "grpc::fetchWorkspace", "%s",
		workspaceName).Out()

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      workspaceName,
	}

	response, err := wsdb.server.FetchWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return key, nonce, false, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return key, nonce, false, wsdb.convertErr(*response.Header)
	}

	key = quantumfs.NewObjectKeyFromBytes(response.Key.GetData())
	nonce = quantumfs.WorkspaceNonce(response.Nonce.Nonce)
	immutable = response.Immutable

	return key, nonce, immutable, nil
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::Workspace").Out()

	workspaceName := typespace + "/" + namespace + "/" + workspace
	key, nonce, _, err := wsdb.fetchWorkspace(c, workspaceName)

	return key, nonce, err
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

	workspaceName := typespace + "/" + namespace + "/" + workspace

	defer c.FuncIn(qlog.LogWorkspaceDb, "grpc::AdvanceWorkspace",
		"%s from %s to %s", workspaceName, currentRootId.String(),
		newRootId.String()).Out()

	request := rpc.AdvanceWorkspaceRequest{
		RequestId:     &rpc.RequestId{Id: c.RequestId},
		WorkspaceName: workspaceName,
		Nonce:         uint64(nonce),
		CurrentRootId: &rpc.ObjectKey{Data: currentRootId.Value()},
		NewRootId:     &rpc.ObjectKey{Data: newRootId.Value()},
	}

	response, err := wsdb.server.AdvanceWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return quantumfs.ObjectKey{}, wsdb.handleGrpcError(err)
	}

	newKey := quantumfs.NewObjectKeyFromBytes(response.NewKey.GetData())

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return newKey, wsdb.convertErr(*response.Header)
	}

	return newKey, nil
}

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "grpc::Workspace").Out()

	workspaceName := typespace + "/" + namespace + "/" + workspace
	_, _, immutable, err := wsdb.fetchWorkspace(c, workspaceName)

	return immutable, err
}

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      typespace + "/" + namespace + "/" + workspace,
	}

	response, err := wsdb.server.SetWorkspaceImmutable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received error %d: %s", response.Err,
			response.ErrCause)
		return wsdb.convertErr(*response)
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
		return wsdb.convertErr(*response)
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
		err := wsdb.convertErr(*response)
		panic(fmt.Sprintf("Unexpected error from server: %s", err.Error()))
	}
}
