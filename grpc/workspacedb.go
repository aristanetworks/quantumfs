// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

import (
	"fmt"
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

const maxRetries = 100

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	wsdbErr, ok := err.(quantumfs.WorkspaceDbErr)
	if !ok {
		return true
	}

	if wsdbErr.Code == quantumfs.WSDB_FATAL_DB_ERROR {
		return true
	}

	return false
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
	conf = maybeAddPort(conf)

	wsdb := &workspaceDB{
		config:           conf,
		subscriptions:    map[string]bool{},
		triggerReconnect: make(chan struct{}),
		waitForReconnect: make(chan struct{}),
	}

	go wsdb.reconnector()

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

	// In order to avoid deadlocks, infinite recursions and a thundering herd of
	// reconnections there exists a single goroutine which performs the
	// reconnection. It waits for notification on the channel, reconnects,
	// replaces the notification channel with a newly instantiated channel and
	// empties the previous channel of all notifications. This is done to prevent
	// stale reconnection requests from causing a new connection to be closed.
	reconnectLock    utils.DeferableMutex
	triggerReconnect chan struct{}
	waitForReconnect chan struct{}
}

// Run in a separate goroutine to trigger reconnection when the connection has failed
func (wsdb *workspaceDB) reconnector() {
	var conn *grpc.ClientConn

	for {
		// Wait for a notification
		<-wsdb.triggerReconnect

		// Reconnect
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

		if conn != nil {
			conn.Close()
			conn = nil
		}

		err := fmt.Errorf("Not an error")
		for err != nil {
			conn, err = grpc.Dial(wsdb.config, connOptions...)
		}

		wsdb.server = rpc.NewWorkspaceDbClient(conn)

		// Swizzle a new notification channel into place and drain the old
		// one
		func() {
			defer wsdb.reconnectLock.Lock().Unlock()
			oldTrigger := wsdb.triggerReconnect
			wsdb.triggerReconnect = make(chan struct{})

			oldWaiter := wsdb.waitForReconnect
			wsdb.waitForReconnect = make(chan struct{})

			for {
				select {
				case <-oldTrigger:
					// Drain other notifications
				default:
					close(oldTrigger)
					close(oldWaiter)
					return
				}
			}
		}()

		// Resync subscriptions
		go wsdb.waitForWorkspaceUpdates()
	}
}

func (wsdb *workspaceDB) reconnect() {
	trigger, wait := func() (chan struct{}, chan struct{}) {
		defer wsdb.reconnectLock.Lock().Unlock()
		return wsdb.triggerReconnect, wsdb.waitForReconnect
	}()

	trigger <- struct{}{}
	<-wait
}

func (wsdb *workspaceDB) waitForWorkspaceUpdates() {
	stream, err := wsdb.server.ListenForUpdates(context.TODO(), &rpc.Void{})
	if err != nil {
		wsdb.reconnect()
		return
	}

	var initialUpdates []*rpc.WorkspaceUpdate
	hitError := func() bool {
		// Replay the current state for all subscribed updates to ensure we
		// haven't missed any notifications while we were disconnected.
		defer wsdb.lock.Lock().Unlock()

		logger, err := qlog.NewQlog("")
		if err != nil {
			fmt.Printf("Error creating qlog file %s", err.Error())
			return true
		}
		ctx := quantumfs.Ctx{
			Qlog:      logger,
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
							Nonce: uint64(nonce),
						},
						Immutable: immutable,
						Deleted:   false,
					})
				continue
			}

			switch err := err.(type) {
			default:
				// Unknown error
				wsdb.reconnect()
				return true
			case quantumfs.WorkspaceDbErr:
				switch err.Code {
				default:
					// Unhandled error
					wsdb.reconnect()
					return true
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
		return false
	}()
	if hitError {
		return
	}

	for {
		var update *rpc.WorkspaceUpdate
		var err error

		if initialUpdates == nil {
			update, err = stream.Recv()
			if err != nil {
				wsdb.reconnect()
				return
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
				Nonce: quantumfs.WorkspaceNonce(
					update.Nonce.Nonce),
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

func (wsdb *workspaceDB) handleGrpcError(err error) error {
	wsdb.reconnect()

	return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_FATAL_DB_ERROR,
		"gRPC failed: %s", err.Error())
}

func (wsdb *workspaceDB) convertErr(response rpc.Response) error {
	return quantumfs.NewWorkspaceDbErr(quantumfs.WsdbErrCode(response.Err),
		response.ErrCause)
}

func logRetry(c *quantumfs.Ctx, attemptNum int, cmd string, err error) {
	if attemptNum < maxRetries-1 {
		c.Dlog(qlog.LogWorkspaceDb,
			"%s failed, retrying: %s", cmd, err.Error())
	}
}

func retry(c *quantumfs.Ctx, opName string, op func(c *quantumfs.Ctx) error) error {
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = op(c)
		if !shouldRetry(err) {
			return err
		}
		logRetry(c, attempt, opName, err)
	}
	c.Dlog(qlog.LogWorkspaceDb, "%s failed, not retrying: %s", opName,
		err.Error())
	return err
}

const NumTypespaceLog = "grpc::NumTypespaces"

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, NumTypespaceLog).Out()

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

const TypespaceListLog = "grpc::TypespaceList"

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	defer c.FuncInName(qlog.LogWorkspaceDb, TypespaceListLog).Out()

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

const NumNamespacesLog = "grpc::NumNamespaces"

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NumNamespacesLog).Out()

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

const NamespaceListLog = "grpc::NamespaceList"

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NamespaceListLog).Out()

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

const NumWorkspacesLog = "grpc::NumWorkspaces"

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, NumWorkspacesLog).Out()

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

const WorkspaceListLog = "grpc::WorkspaceList"

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, WorkspaceListLog).Out()

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

const BranchWorkspaceLog = "grpc::BranchWorkspace"

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, BranchWorkspaceLog).Out()

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

const DeleteWorkspaceLog = "grpc::DeleteWorkspace"

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, DeleteWorkspaceLog).Out()

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

	response, err := wsdb.server.FetchWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return quantumfs.ZeroKey, 0, false, wsdb.handleGrpcError(err)
	}

	if response.Header.Err != 0 {
		c.Vlog(qlog.LogWorkspaceDb, "Received wsdb error %d: %s",
			response.Header.Err, response.Header.ErrCause)
		return quantumfs.ZeroKey, 0, false, wsdb.convertErr(*response.Header)
	}

	key = quantumfs.NewObjectKeyFromBytes(response.Key.GetData())
	nonce = quantumfs.WorkspaceNonce(response.Nonce.Nonce)
	immutable = response.Immutable

	return key, nonce, immutable, nil
}

const WorkspaceLog = "grpc::Workspace"

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, WorkspaceLog).Out()

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

	err := wsdb.SubscribeTo(typespace + "/" + namespace + "/" + workspace)
	if err != nil {
		return quantumfs.ZeroKey, 0, err
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
		Nonce:         &rpc.WorkspaceNonce{Nonce: uint64(nonce)},
		CurrentRootId: &rpc.ObjectKey{Data: currentRootId.Value()},
		NewRootId:     &rpc.ObjectKey{Data: newRootId.Value()},
	}

	response, err := wsdb.server.AdvanceWorkspace(context.TODO(), &request)
	if err != nil {
		c.Vlog(qlog.LogWorkspaceDb, "Received grpc error: %s", err.Error())
		return quantumfs.ZeroKey, wsdb.handleGrpcError(err)
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
	func() {
		defer wsdb.lock.Lock().Unlock()
		wsdb.subscriptions[workspaceName] = true
	}()

	return wsdb.subscribeTo(workspaceName)
}

func (wsdb *workspaceDB) subscribeTo(workspaceName string) error {
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = wsdb._subscribeTo(workspaceName)
		if !shouldRetry(err) {
			return err
		}
	}

	panic("subscribeTo failed, no more retries")
}

func (wsdb *workspaceDB) _subscribeTo(workspaceName string) error {
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
	func() {
		defer wsdb.lock.Lock().Unlock()
		delete(wsdb.subscriptions, workspaceName)
	}()

	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = wsdb.unsubscribeFrom(workspaceName)
		if !shouldRetry(err) {
			return
		}
	}

	panic("UnsubscribeFrom failed, no more retries")
}

func (wsdb *workspaceDB) unsubscribeFrom(workspaceName string) error {
	request := rpc.WorkspaceName{
		RequestId: &rpc.RequestId{Id: 0},
		Name:      workspaceName,
	}

	response, err := wsdb.server.UnsubscribeFrom(context.TODO(), &request)
	if err != nil {
		return wsdb.handleGrpcError(err)
	}

	if response.Err != 0 {
		return wsdb.convertErr(*response)
	}
	return nil
}
