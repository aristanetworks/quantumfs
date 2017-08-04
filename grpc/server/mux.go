// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/utils"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
)

type Server struct {
	server *grpc.Server
	Error  error // Error after serving ceases
}

func (server *Server) Stop() error {
	server.server.Stop()
	return server.Error
}

type workspaceState struct {
	name string
	data quantumfs.WorkspaceState
}

// Start the WorkspaceDBd goroutine. This will open a socket and list on the given
// port until an error occurs.
//
// backend is a string specifying which backend to use, currently ether.cql and
// systemlocal are the only supported backends.
//
// config is the configuration string to pass to that backend.
func StartWorkspaceDbd(logger *qlog.Qlog, port uint16, backend string,
	config string) (*Server, error) {

	logger.Log(qlog.LogWorkspaceDb, 0, 2,
		"Starting grpc WorkspaceDB Server on port %d against backend %s",
		port, backend)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	connOptions := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             1 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    5 * time.Second,
			Timeout: 1 * time.Second,
		}),
	}

	wsdb, err := thirdparty_backends.ConnectWorkspaceDB(backend, config)
	if err != nil {
		logger.Log(qlog.LogWorkspaceDb, 0, 0,
			"Failed to instantiate backend: %s", err.Error())
		return nil, err
	}

	m := newMux(wsdb, logger)

	grpcServer := grpc.NewServer(connOptions...)
	rpc.RegisterWorkspaceDbServer(grpcServer, m)

	s := &Server{
		server: grpcServer,
	}

	go func() {
		logger.Log(qlog.LogWorkspaceDb, 0, 2, "Serving clients")
		s.Error = grpcServer.Serve(listener)

		if s.Error == nil {
			logger.Log(qlog.LogWorkspaceDb, 0, 2,
				"Finished serving clients")
		} else {
			logger.Log(qlog.LogWorkspaceDb, 0, 2,
				"Finished serving clients with error %s",
				s.Error.Error())
		}
	}()

	return s, nil
}

// mux is the central component of the Workspace DB daemon. It receives the RPCs from
// the clients, reads/writes to the persistent database and sends notifications to
// all subscribed clients.
type mux struct {
	subscriptionLock utils.DeferableRwMutex
	// workspace name to client name
	subscriptionsByWorkspace map[string]map[string]bool
	subscriptionsByClient    map[string]map[string]bool
	// client name to notification channel
	clients map[string]chan workspaceState

	// For now simply use one of the existing backends
	backend quantumfs.WorkspaceDB

	qlog          *qlog.Qlog
	lastRequestId uint64
}

func newMux(wsdb quantumfs.WorkspaceDB, qlog *qlog.Qlog) *mux {
	m := mux{
		subscriptionsByWorkspace: map[string]map[string]bool{},
		subscriptionsByClient:    map[string]map[string]bool{},
		clients:                  map[string]chan workspaceState{},
		backend:                  wsdb,
		qlog:                     qlog,
	}

	return &m
}

func (m *mux) newCtx(remoteRequestId uint64, context context.Context) *ctx {
	requestId := atomic.AddUint64(&m.lastRequestId, 1)
	clientName := "unknown"
	client, ok := peer.FromContext(context)
	if ok {
		clientName = client.Addr.String()
	}

	c := &ctx{
		Ctx: quantumfs.Ctx{
			Qlog:      m.qlog,
			RequestId: requestId,
		},
		clientName: clientName,
	}

	c.vlog("Starting remote request (%s: %d)", clientName, remoteRequestId)

	return c
}

func (m *mux) NumTypespaces(ctx context.Context, request *rpc.RequestId) (
	*rpc.NumTypespacesResponse, error) {

	c := m.newCtx(request.Id, ctx)
	defer c.funcIn("mux::NumTypespacesResponse").Out()

	num, err := m.backend.NumTypespaces(&c.Ctx)

	response := rpc.NumTypespacesResponse{
		Header: &rpc.Response{
			RequestId: request,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
		NumTypespaces: 0,
	}

	if err == nil {
		response.Header.Err = 0
		response.NumTypespaces = int64(num)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) TypespaceTable(ctx context.Context, request *rpc.RequestId) (
	*rpc.TypespaceTableResponse, error) {

	c := m.newCtx(request.Id, ctx)
	defer c.funcIn("mux::TypespaceTable").Out()

	typespaces, err := m.backend.TypespaceList(&c.Ctx)

	response := rpc.TypespaceTableResponse{
		Header: &rpc.Response{
			RequestId: request,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
	}

	if err == nil {
		response.Header.Err = 0
		response.Typespaces = typespaces
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error %s", err.Error())
	return &response, err
}

func (m *mux) NumNamespaces(ctx context.Context, request *rpc.NamespaceRequest) (
	*rpc.NumNamespacesResponse, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::NumNamespaces", "typespace %s", request.Typespace).Out()

	num, err := m.backend.NumNamespaces(&c.Ctx, request.Typespace)

	response := rpc.NumNamespacesResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
		NumNamespaces: 0,
	}

	if err == nil {
		response.Header.Err = 0
		response.NumNamespaces = int64(num)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) NamespaceTable(ctx context.Context, request *rpc.NamespaceRequest) (
	*rpc.NamespaceTableResponse, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::NamespaceTable", "typespace %s",
		request.Typespace).Out()

	namespaces, err := m.backend.NamespaceList(&c.Ctx, request.Typespace)

	response := rpc.NamespaceTableResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
	}

	if err == nil {
		response.Header.Err = 0
		response.Namespaces = namespaces
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) NumWorkspaces(ctx context.Context, request *rpc.WorkspaceRequest) (
	*rpc.NumWorkspacesResponse, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::NumWorkspaces", "typespace %s namespace %s",
		request.Typespace, request.Namespace).Out()

	num, err := m.backend.NumWorkspaces(&c.Ctx, request.Typespace,
		request.Namespace)

	response := rpc.NumWorkspacesResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
		NumWorkspaces: 0,
	}

	if err == nil {
		response.Header.Err = 0
		response.NumWorkspaces = int64(num)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) WorkspaceTable(ctx context.Context, request *rpc.WorkspaceRequest) (
	*rpc.WorkspaceTableResponse, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::WorkspaceTable", "typespace %s namespace %s",
		request.Typespace, request.Namespace).Out()

	workspaceNonces, err := m.backend.WorkspaceList(&c.Ctx, request.Typespace,
		request.Namespace)

	response := rpc.WorkspaceTableResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
	}

	if err == nil {
		nonces := make(map[string]*rpc.WorkspaceNonce, len(workspaceNonces))
		for name, nonce := range workspaceNonces {
			nonces[name] = &rpc.WorkspaceNonce{Nonce: uint64(nonce)}
		}

		response.Header.Err = 0
		response.Workspaces = nonces
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) SubscribeTo(ctx context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::SubscribeTo", "%s", request.Name).Out()

	defer m.subscriptionLock.Lock().Unlock()

	if _, ok := m.subscriptionsByWorkspace[request.Name]; !ok {
		c.vlog("Creating workspace subscriptions map")
		m.subscriptionsByWorkspace[request.Name] = map[string]bool{}
	}

	if _, ok := m.subscriptionsByClient[c.clientName]; !ok {
		c.vlog("Creating client subscriptions map")
		m.subscriptionsByClient[c.clientName] = map[string]bool{}
	}

	m.subscriptionsByWorkspace[request.Name][c.clientName] = true
	m.subscriptionsByClient[c.clientName][request.Name] = true

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       0,
		ErrCause:  "Success",
	}
	return &response, nil
}

func (m *mux) UnsubscribeFrom(ctx context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::UnsubscribeFrom", "%s", request.Name).Out()

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       0,
		ErrCause:  "Success",
	}

	defer m.subscriptionLock.Lock().Unlock()

	if _, ok := m.subscriptionsByClient[c.clientName]; !ok {
		c.vlog("Client is not subscribed to any workspace names")
		return &response, nil
	}

	_, subscribed := m.subscriptionsByClient[c.clientName][request.Name]
	if !subscribed {
		c.vlog("Client is not subscribed to workspace")
		return &response, nil
	}

	delete(m.subscriptionsByClient[c.clientName], request.Name)

	if _, ok := m.subscriptionsByWorkspace[request.Name]; ok {
		delete(m.subscriptionsByWorkspace[request.Name], c.clientName)
	}

	if len(m.subscriptionsByWorkspace[request.Name]) == 0 {
		c.vlog("Deleting workspace subscriptions map")
		delete(m.subscriptionsByWorkspace, request.Name)
	}

	if len(m.subscriptionsByClient[c.clientName]) == 0 {
		c.vlog("Deleting client subscription map")
		delete(m.subscriptionsByClient, c.clientName)
	}

	return &response, nil
}

func (m *mux) ListenForUpdates(_ *rpc.Void,
	stream rpc.WorkspaceDb_ListenForUpdatesServer) error {

	c := m.newCtx(0, stream.Context())
	defer c.funcIn("mux::ListenForUpdates").Out()

	changes := make(chan workspaceState, 128)

	func() {
		defer m.subscriptionLock.Lock().Unlock()
		m.clients[c.clientName] = changes
	}()
	c.vlog("Registered client")

	defer func() {
		c.vlog("Unregistering client")
		defer m.subscriptionLock.Lock().Unlock()
		delete(m.clients, c.clientName)

		for workspace, _ := range m.subscriptionsByClient[c.clientName] {
			if _, ok := m.subscriptionsByWorkspace[workspace]; ok {
				delete(m.subscriptionsByWorkspace[workspace],
					c.clientName)
			}
		}
		delete(m.subscriptionsByClient, c.clientName)
	}()

	for {
		select {
		case change := <-changes:
			c.vlog("Received update for %s", change.name)
			update := rpc.WorkspaceUpdate{
				Name: change.name,
				RootId: &rpc.ObjectKey{
					Data: change.data.RootId.Value(),
				},
				Nonce: &rpc.WorkspaceNonce{
					Nonce: uint64(change.data.Nonce),
				},
				Immutable: change.data.Immutable,
				Deleted:   change.data.Deleted,
			}
			err := stream.Send(&update)
			if err != nil {
				c.vlog("Recevied stream send error: %s", err.Error())
				return err
			}
		}
	}

	panic("ListenForUpdates terminated unexpectedly")
	return nil
}

func (m *mux) notifyChange(c *ctx, workspaceName string, requestId *rpc.RequestId,
	deleted bool) {

	defer c.FuncIn("mux::notifyChange", "workspace %s deleted %t", workspaceName,
		deleted).Out()

	var update workspaceState
	update.name = workspaceName

	if deleted {
		update.data.Deleted = true
	} else {
		parts := strings.Split(workspaceName, "/")
		key, nonce, err := m.backend.Workspace(&c.Ctx, parts[0], parts[1],
			parts[2])

		if err != nil {
			panic("Received error when fetching workspace")
		}

		update.data.RootId = key
		update.data.Nonce = nonce
		update.data.Immutable = false
		update.data.Deleted = false
	}

	defer m.subscriptionLock.RLock().RUnlock()
	subscriptions, ok := m.subscriptionsByWorkspace[workspaceName]

	if !ok {
		c.vlog("Nobody is subscribed to this workspace")
		return
	}

	for clientName, _ := range subscriptions {
		if client, ok := m.clients[clientName]; ok {
			c.vlog("Sending update to client %s", clientName)
			client <- update
		}
	}
}

func (m *mux) FetchWorkspace(ctx context.Context, request *rpc.WorkspaceName) (
	*rpc.FetchWorkspaceResponse, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::FetchWorkspace", "%s", request.Name).Out()

	parts := strings.Split(request.Name, "/")
	key, nonce, err := m.backend.Workspace(&c.Ctx, parts[0], parts[1], parts[2])

	response := rpc.FetchWorkspaceResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
		Key:       &rpc.ObjectKey{},
		Nonce:     &rpc.WorkspaceNonce{Nonce: 0},
		Immutable: false,
	}

	if err == nil {
		response.Header.Err = 0
		response.Key.Data = key.Value()
		response.Nonce.Nonce = uint64(nonce)
		response.Immutable = false
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) BranchWorkspace(ctx context.Context,
	request *rpc.BranchWorkspaceRequest) (*rpc.Response, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::BranchWorkspace", "%s -> %s", request.Source,
		request.Destination).Out()

	srcParts := strings.Split(request.Source, "/")
	dstParts := strings.Split(request.Destination, "/")
	err := m.backend.BranchWorkspace(&c.Ctx, srcParts[0], srcParts[1],
		srcParts[2], dstParts[0], dstParts[1], dstParts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(c, request.Destination, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) DeleteWorkspace(ctx context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::DeleteWorkspace", "%s", request.Name).Out()

	parts := strings.Split(request.Name, "/")
	err := m.backend.DeleteWorkspace(&c.Ctx, parts[0], parts[1], parts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(c, request.Name, request.RequestId, true)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) SetWorkspaceImmutable(ctx context.Context,
	request *rpc.WorkspaceName) (*rpc.Response, error) {

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::SetWorkspaceImmutable", "%s", request.Name).Out()

	parts := strings.Split(request.Name, "/")
	err := m.backend.SetWorkspaceImmutable(&c.Ctx, parts[0], parts[1], parts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(c, request.Name, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}

func (m *mux) AdvanceWorkspace(ctx context.Context,
	request *rpc.AdvanceWorkspaceRequest) (
	*rpc.AdvanceWorkspaceResponse, error) {

	currentKey := quantumfs.NewObjectKeyFromBytes(request.CurrentRootId.Data)
	newKey := quantumfs.NewObjectKeyFromBytes(request.NewRootId.Data)
	nonce := quantumfs.WorkspaceNonce(request.Nonce.Nonce)

	c := m.newCtx(request.RequestId.Id, ctx)
	defer c.FuncIn("mux::AdvanceWorkspace", "workspace %s (%d): %s -> %s",
		request.WorkspaceName, nonce, currentKey.String(),
		newKey.String()).Out()

	parts := strings.Split(request.WorkspaceName, "/")
	dbKey, err := m.backend.AdvanceWorkspace(&c.Ctx, parts[0], parts[1],
		parts[2], nonce, currentKey, newKey)

	response := rpc.AdvanceWorkspaceResponse{
		Header: &rpc.Response{
			RequestId: request.RequestId,
			Err:       quantumfs.WSDB_FATAL_DB_ERROR,
			ErrCause:  "Unknown",
		},
	}

	if err == nil {
		response.Header.Err = 0
		response.Header.ErrCause = "Success"
		response.NewKey = &rpc.ObjectKey{Data: dbKey.Value()}
		m.notifyChange(c, request.WorkspaceName, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		c.vlog("Received workspaceDB error %d: %s", err.Code, err.Msg)
		return &response, nil
	}

	c.vlog("Received other error: %s", err.Error())
	return &response, err
}
