// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

import (
	"fmt"
	"runtime/debug"
	"strings"
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

const retryDelay = 50 * time.Millisecond

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// WSDB errors are not expected to improve with retries
	_, ok := err.(quantumfs.WorkspaceDbErr)
	if ok {
		return false
	}

	// All other errors are assumed to be either network or service related and
	// therefore will eventually clear if we retry long enough.
	return true
}

func maybeAddPort(conf string) string {
	lastColon := strings.LastIndex(conf, ":")
	lastBracket := strings.LastIndex(conf, "]")

	if lastBracket != -1 { // IPv6
		if lastColon == -1 || lastColon < lastBracket {
			conf = fmt.Sprintf("%s:%d", conf, rpc.Ports_Default)
		}
	} else { // IPv4 or hostname
		if lastColon == -1 {
			conf = fmt.Sprintf("%s:%d", conf, rpc.Ports_Default)
		}
	}

	return conf
}

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	return newWorkspaceDB_(conf, grpcConnect)
}

func newWorkspaceDB_(conf string, connectFn func(*grpc.ClientConn,
	string) (*grpc.ClientConn, rpc.WorkspaceDbClient)) quantumfs.WorkspaceDB {

	conf = maybeAddPort(conf)

	qlog, err := qlog.NewQlog("")
	utils.Assert(err == nil, "Unable to create empty Qlog")

	wsdb := &workspaceDB{
		connectFn:     connectFn,
		config:        conf,
		subscriptions: map[string]bool{},
		qlog:          qlog,
		server:        newServerContainer(),
		// There is no need to block on triggering a reconnect. Making this
		// a buffered channel will ensure that concurrent grpc users
		// aren't occasionally blocked on reconnect contention
		triggerReconnect: make(chan badConnectionInfo, 1000),
	}

	connected := make(chan struct{})
	go wsdb.reconnector(connected)
	wsdb.reconnect(0)
	// wait for the server to be connected before we start the updater
	<-connected

	go wsdb.updater()

	return wsdb
}

type serverSnapshotter interface {
	Snapshot() (*rpc.WorkspaceDbClient, uint32)
	ReplaceServer(*rpc.WorkspaceDbClient)
}

type serverContainer struct {
	lock          utils.DeferableMutex
	server        *rpc.WorkspaceDbClient
	serverConnIdx uint32
}

func newServerContainer() serverSnapshotter {
	return &serverContainer{
		server: nil,
	}
}

func (srv *serverContainer) Snapshot() (*rpc.WorkspaceDbClient, uint32) {
	defer srv.lock.Lock().Unlock()

	return srv.server, srv.serverConnIdx
}

func (srv *serverContainer) ReplaceServer(server *rpc.WorkspaceDbClient) {
	defer srv.lock.Lock().Unlock()

	srv.server = server
	srv.serverConnIdx++
}

type badConnectionInfo struct {
	idx uint32
}

type workspaceDB struct {
	// This is our connection function, which allows us to stub during tests
	connectFn func(*grpc.ClientConn, string) (*grpc.ClientConn,
		rpc.WorkspaceDbClient)

	config        string
	lock          utils.DeferableMutex
	callback      quantumfs.SubscriptionCallback
	subscriptions map[string]bool
	updates       map[string]quantumfs.WorkspaceState

	qlog *qlog.Qlog

	server serverSnapshotter

	triggerReconnect chan badConnectionInfo
}

func grpcConnect(old *grpc.ClientConn, config string) (newConn *grpc.ClientConn,
	newClient rpc.WorkspaceDbClient) {

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

	if old != nil {
		old.Close()
	}

	var rtn *grpc.ClientConn
	err := fmt.Errorf("Not an error")
	for err != nil {
		rtn, err = grpc.Dial(config, connOptions...)
	}

	return rtn, rpc.NewWorkspaceDbClient(rtn)
}

// Run in a separate goroutine to trigger reconnection when the connection has failed
func (wsdb *workspaceDB) reconnector(started chan struct{}) {
	var conn *grpc.ClientConn

	initialized := false
	for {
		// Wait for a notification
		badConnection := <-wsdb.triggerReconnect

		// If this notification is stale, throw it away
		_, serverConnIdx := wsdb.server.Snapshot()
		if badConnection.idx < serverConnIdx {
			continue
		}

		utils.Assert(badConnection.idx == serverConnIdx,
			"badConnection from the future %d %d", badConnection.idx,
			serverConnIdx)

		// Reconnect
		var newServer rpc.WorkspaceDbClient
		conn, newServer = wsdb.connectFn(conn, wsdb.config)

		wsdb.server.ReplaceServer(&newServer)

		if !initialized {
			started <- struct{}{}
			initialized = true
		}
	}
}

func (wsdb *workspaceDB) reconnect(badConnIdx uint32) {
	wsdb.triggerReconnect <- badConnectionInfo{
		idx: badConnIdx,
	}
}

// An always running, singular goroutine to update listeners when workspacedb updates
func (wsdb *workspaceDB) updater() {
	for {
		err := wsdb._update()

		// we must have gotten an error for _update to return, so output it
		wsdb.qlog.Log(qlog.LogWorkspaceDb, 0, 0,
			"grpc::workspaceDB connection error: %s", err)
	}
}

// This function would only ever return due to an error
func (wsdb *workspaceDB) _update() (rtnErr error) {
	defer func() {
		exception := recover()
		if exception != nil {
			// Log
			stackTrace := debug.Stack()
			rtnErr = fmt.Errorf("Panic in grpc::waitForWorkspace"+
				"Updates:\n%s\n", utils.BytesToString(stackTrace))
		}
	}()

	server, serverConnIdx := wsdb.server.Snapshot()
	defer wsdb.reconnect(serverConnIdx)

	stream, err := (*server).ListenForUpdates(context.TODO(), &rpc.Void{})
	if err != nil {
		return err
	}

	var initialUpdates []*rpc.WorkspaceUpdate
	err = func() error {
		// Replay the current state for all subscribed updates to ensure we
		// haven't missed any notifications while we were disconnected.
		defer wsdb.lock.Lock().Unlock()

		ctx := quantumfs.Ctx{
			Qlog:      wsdb.qlog,
			RequestId: uint64(rpc.ReservedRequestIds_RESYNC),
		}
		for workspace, _ := range wsdb.subscriptions {
			wsdb.subscribeTo(workspace)
			key, nonce, immutable, err := wsdb.fetchWorkspace(&ctx,
				workspace)

			if err == nil {
				initialUpdates = append(initialUpdates,
					&rpc.WorkspaceUpdate{
						Name: workspace,
						RootId: &rpc.ObjectKey{
							Data: key.Value(),
						},
						Nonce: &rpc.WorkspaceNonce{
							Id: uint64(nonce.Id),
							PublishTime: uint64(
								nonce.PublishTime),
						},
						Immutable: immutable,
						Deleted:   false,
					})
				continue
			}

			switch err := err.(type) {
			default:
				return err
			case quantumfs.WorkspaceDbErr:
				switch err.Code {
				default:
					return err
				case quantumfs.WSDB_WORKSPACE_NOT_FOUND:
					// Workspace may have been deleted
					zero := quantumfs.ZeroKey
					deleted := &rpc.WorkspaceUpdate{
						Name: workspace,
						RootId: &rpc.ObjectKey{
							Data: zero.Value(),
						},
						Nonce:   &rpc.WorkspaceNonce{},
						Deleted: true,
					}
					initialUpdates = append(initialUpdates,
						deleted)
					continue
				}
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	for {
		var update *rpc.WorkspaceUpdate
		var err error

		if initialUpdates == nil {
			update, err = stream.Recv()
			if err != nil {
				return err
			}
		} else {
			update = initialUpdates[0]
			initialUpdates = initialUpdates[1:]
			if len(initialUpdates) == 0 {
				initialUpdates = nil
			}
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
				wsdb.updates = map[string]quantumfs.WorkspaceState{}
			}

			wsdb.updates[update.Name] = quantumfs.WorkspaceState{
				RootId: quantumfs.NewObjectKeyFromBytes(
					update.RootId.Data),
				Nonce: quantumfs.WorkspaceNonce{
					Id:          update.Nonce.Id,
					PublishTime: update.Nonce.PublishTime,
				},
				Immutable: update.Immutable,
				Deleted:   update.Deleted,
			}

			return true
		}()

		if subscribed && startTransmission {
			go wsdb.sendNotifications()
		}
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

func (wsdb *workspaceDB) handleGrpcError(err error, serverConnIdx uint32) error {
	wsdb.reconnect(serverConnIdx)

	return err
}

func (wsdb *workspaceDB) convertErr(response rpc.Response) error {
	return quantumfs.NewWorkspaceDbErr(quantumfs.WsdbErrCode(response.Err),
		response.ErrCause)
}

func retry(c *quantumfs.Ctx, opName string, op func(c *quantumfs.Ctx) error) error {
	var err error
	for {
		err = op(c)
		if !shouldRetry(err) {
			return err
		}
		c.Dlog(qlog.LogWorkspaceDb,
			"%s failed, retrying: %s", opName, err.Error())
		time.Sleep(retryDelay)
	}
}

const NumTypespaceLog = "grpc::NumTypespaces"

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, NumTypespaceLog).Out()

	wsdb.qlog = c.Qlog
	var result int

	err := retry(c, "NumTypespaces", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.numTypespaces(c)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) numTypespaces(c *quantumfs.Ctx) (int, error) {
	request := rpc.RequestId{Id: c.RequestId}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).NumTypespaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumTypespaces), nil
}

const TypespaceListLog = "grpc::TypespaceList"

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, TypespaceListLog).Out()

	wsdb.qlog = c.Qlog
	var result []string

	err := retry(c, "TypespaceList", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.typespaceList(c)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) typespaceList(c *quantumfs.Ctx) ([]string, error) {
	request := rpc.RequestId{Id: c.RequestId}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).TypespaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return []string{}, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return []string{}, wsdb.convertErr(*response.Header)
	}

	return response.Typespaces, nil
}

const NumNamespacesLog = "grpc::NumNamespaces"

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NumNamespacesLog).Out()
	wsdb.qlog = c.Qlog

	var result int

	err := retry(c, "NumNamespaces", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.numNamespaces(c, typespace)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) numNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	request := rpc.NamespaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).NumNamespaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumNamespaces), nil
}

const NamespaceListLog = "grpc::NamespaceList"

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NamespaceListLog).Out()
	wsdb.qlog = c.Qlog

	var result []string

	err := retry(c, "NamespaceList", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.namespaceList(c, typespace)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) namespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	request := rpc.NamespaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).NamespaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return []string{}, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return []string{}, wsdb.convertErr(*response.Header)
	}

	return response.Namespaces, nil
}

const NumWorkspacesLog = "grpc::NumWorkspaces"

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NumWorkspacesLog).Out()
	wsdb.qlog = c.Qlog

	var result int

	err := retry(c, "NumWorkspaces", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.numWorkspaces(c, typespace, namespace)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) numWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	request := rpc.WorkspaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).NumWorkspaces(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return 0, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return 0, wsdb.convertErr(*response.Header)
	}

	return int(response.NumWorkspaces), nil
}

const WorkspaceListLog = "grpc::WorkspaceList"

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, WorkspaceListLog).Out()
	wsdb.qlog = c.Qlog

	var result map[string]quantumfs.WorkspaceNonce

	err := retry(c, "WorkspaceList", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.workspaceList(c, typespace, namespace)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) workspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	request := rpc.WorkspaceRequest{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Typespace: typespace,
		Namespace: namespace,
	}

	workspaces := map[string]quantumfs.WorkspaceNonce{}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).WorkspaceTable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return workspaces, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return workspaces, wsdb.convertErr(*response.Header)
	}

	for name, nonce := range response.Workspaces {
		workspaces[name] = quantumfs.WorkspaceNonce{
			Id:          nonce.Id,
			PublishTime: nonce.PublishTime,
		}
	}

	return workspaces, nil
}

const BranchWorkspaceLog = "grpc::BranchWorkspace"

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, BranchWorkspaceLog).Out()
	wsdb.qlog = c.Qlog

	err := retry(c, "BranchWorkspace", func(c *quantumfs.Ctx) error {
		return wsdb.branchWorkspace(c, srcTypespace, srcNamespace,
			srcWorkspace, dstTypespace, dstNamespace, dstWorkspace)
	})

	return err
}

func (wsdb *workspaceDB) branchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	request := rpc.BranchWorkspaceRequest{
		RequestId:   &rpc.RequestId{Id: c.RequestId},
		Source:      srcTypespace + "/" + srcNamespace + "/" + srcWorkspace,
		Destination: dstTypespace + "/" + dstNamespace + "/" + dstWorkspace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).BranchWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Err, response.ErrCause)
		return wsdb.convertErr(*response)
	}

	return nil
}

const DeleteWorkspaceLog = "grpc::DeleteWorkspace"

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, DeleteWorkspaceLog).Out()
	wsdb.qlog = c.Qlog

	err := retry(c, "DeleteWorkspace", func(c *quantumfs.Ctx) error {
		return wsdb.deleteWorkspace(c, typespace, namespace, workspace)
	})

	return err
}

func (wsdb *workspaceDB) deleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      typespace + "/" + namespace + "/" + workspace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).DeleteWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Err, response.ErrCause)
		return wsdb.convertErr(*response)
	}

	return nil
}

const FetchWorkspaceLog = "grpc::fetchWorkspace"
const FetchWorkspaceDebug = "%s"

func (wsdb *workspaceDB) fetchWorkspace(c *quantumfs.Ctx, workspaceName string) (
	key quantumfs.ObjectKey, nonce quantumfs.WorkspaceNonce, immutable bool,
	err error) {

	defer c.FuncIn(qlog.LogWorkspaceDb, FetchWorkspaceLog, FetchWorkspaceDebug,
		workspaceName).Out()

	err = retry(c, "fetchWorkspace", func(c *quantumfs.Ctx) error {
		var err error
		key, nonce, immutable, err = wsdb._fetchWorkspace(c, workspaceName)
		return err
	})

	return key, nonce, immutable, err
}

func (wsdb *workspaceDB) _fetchWorkspace(c *quantumfs.Ctx, workspaceName string) (
	key quantumfs.ObjectKey, nonce quantumfs.WorkspaceNonce, immutable bool,
	err error) {

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      workspaceName,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).FetchWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return quantumfs.ZeroKey, quantumfs.WorkspaceNonce{},
			false, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return quantumfs.ZeroKey, quantumfs.WorkspaceNonce{},
			false, wsdb.convertErr(*response.Header)
	}

	key = quantumfs.NewObjectKeyFromBytes(response.Key.GetData())
	nonce = quantumfs.WorkspaceNonce{
		Id:          response.Nonce.Id,
		PublishTime: response.Nonce.PublishTime,
	}
	immutable = response.Immutable

	return key, nonce, immutable, nil
}

const WorkspaceLog = "grpc::Workspace"

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, WorkspaceLog).Out()
	wsdb.qlog = c.Qlog

	var resKey quantumfs.ObjectKey
	var resNonce quantumfs.WorkspaceNonce

	err := retry(c, "Workspace", func(c *quantumfs.Ctx) error {
		var err error
		resKey, resNonce, err = wsdb.workspace(c, typespace, namespace,
			workspace)
		return err
	})

	return resKey, resNonce, err
}

func (wsdb *workspaceDB) workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	workspaceName := typespace + "/" + namespace + "/" + workspace
	key, nonce, _, err := wsdb.fetchWorkspace(c, workspaceName)

	return key, nonce, err
}

func (wsdb *workspaceDB) FetchAndSubscribeWorkspace(c *quantumfs.Ctx,
	typespace string, namespace string, workspace string) (
	quantumfs.ObjectKey, quantumfs.WorkspaceNonce, error) {

	wsdb.qlog = c.Qlog
	err := wsdb.SubscribeTo(typespace + "/" + namespace + "/" + workspace)
	if err != nil {
		return quantumfs.ZeroKey, quantumfs.WorkspaceNonce{}, err
	}

	return wsdb.Workspace(c, typespace, namespace, workspace)
}

const AdvanceWorkspaceLog = "grpc::AdvanceWorkspace"
const AdvanceWorkspaceDebug = "%s from %s to %s"

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, nonce quantumfs.WorkspaceNonce,
	currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	workspaceName := typespace + "/" + namespace + "/" + workspace

	defer c.FuncIn(qlog.LogWorkspaceDb, AdvanceWorkspaceLog,
		AdvanceWorkspaceDebug, workspaceName, currentRootId.String(),
		newRootId.String()).Out()

	wsdb.qlog = c.Qlog
	var result quantumfs.ObjectKey

	err := retry(c, "AdvanceWorkspace", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.advanceWorkspace(c, typespace, namespace,
			workspace, nonce, currentRootId, newRootId)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) advanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, nonce quantumfs.WorkspaceNonce,
	currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	workspaceName := typespace + "/" + namespace + "/" + workspace

	request := rpc.AdvanceWorkspaceRequest{
		RequestId:     &rpc.RequestId{Id: c.RequestId},
		WorkspaceName: workspaceName,
		Nonce: &rpc.WorkspaceNonce{
			Id:          nonce.Id,
			PublishTime: nonce.PublishTime,
		},
		CurrentRootId: &rpc.ObjectKey{Data: currentRootId.Value()},
		NewRootId:     &rpc.ObjectKey{Data: newRootId.Value()},
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).AdvanceWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return quantumfs.ZeroKey, wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return quantumfs.ZeroKey, wsdb.convertErr(*response.Header)
	}

	newKey := quantumfs.NewObjectKeyFromBytes(response.NewKey.GetData())
	return newKey, nil
}

const WorkspaceIsImmutableLog = "grpc::WorkspaceIsImmutable"

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, WorkspaceIsImmutableLog).Out()
	wsdb.qlog = c.Qlog

	var result bool

	err := retry(c, "WorkspaceIsImmutable", func(c *quantumfs.Ctx) error {
		var err error
		result, err = wsdb.workspaceIsImmutable(c, typespace, namespace,
			workspace)
		return err
	})

	return result, err
}

func (wsdb *workspaceDB) workspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	workspaceName := typespace + "/" + namespace + "/" + workspace
	_, _, immutable, err := wsdb.fetchWorkspace(c, workspaceName)

	return immutable, err
}

const SetWorkspaceImmutableLog = "grpc::SetWorkspaceImmutable"
const SetWorkspaceImmutableDebug = "%s/%s/%s"

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn(qlog.LogWorkspaceDb, SetWorkspaceImmutableLog,
		SetWorkspaceImmutableDebug, typespace, namespace, workspace).Out()
	wsdb.qlog = c.Qlog

	err := retry(c, "SetWorkspaceImmutable", func(c *quantumfs.Ctx) error {
		return wsdb.setWorkspaceImmutable(c, typespace, namespace, workspace)
	})

	return err
}

func (wsdb *workspaceDB) setWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: c.RequestId},
		Name:      typespace + "/" + namespace + "/" + workspace,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).SetWorkspaceImmutable(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return wsdb.handleGrpcError(err, serverConnIdx)
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
	func() {
		defer wsdb.lock.Lock().Unlock()
		wsdb.subscriptions[workspaceName] = true
	}()

	return wsdb.subscribeTo(workspaceName)
}

func (wsdb *workspaceDB) subscribeTo(workspaceName string) error {
	var err error

	for {
		err = wsdb._subscribeTo(workspaceName)
		if !shouldRetry(err) {
			return err
		}
		time.Sleep(retryDelay)
	}
}

func (wsdb *workspaceDB) _subscribeTo(workspaceName string) error {
	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: 0},
		Name:      workspaceName,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).SubscribeTo(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Err != 0 {
		return wsdb.convertErr(*response)
	}

	return nil
}

func (wsdb *workspaceDB) UnsubscribeFrom(workspaceName string) {
	func() {
		defer wsdb.lock.Lock().Unlock()
		delete(wsdb.subscriptions, workspaceName)
	}()

	var err error

	for {
		err = wsdb.unsubscribeFrom(workspaceName)
		if !shouldRetry(err) {
			return
		}
		time.Sleep(retryDelay)
	}
}

func (wsdb *workspaceDB) unsubscribeFrom(workspaceName string) error {
	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: 0},
		Name:      workspaceName,
	}

	server, serverConnIdx := wsdb.server.Snapshot()

	response, err := (*server).UnsubscribeFrom(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err, serverConnIdx)
	}

	if response.Err != 0 {
		return wsdb.convertErr(*response)
	}
	return nil
}
