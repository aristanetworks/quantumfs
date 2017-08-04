// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"fmt"
	"net"
	"strings"
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

// Start the WorkspaceDBd goroutine. This will open a socket and list on the given
// port until an error occurs.
//
// backend is a string specifying which backend to use, currently ether.cql and
// systemlocal are the only supported backends.
//
// config is the configuration string to pass to that backend.
func StartWorkspaceDbd(port uint16, backend string, config string) (
	*Server, error) {
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
		return nil, err
	}

	m := newMux(wsdb)

	grpcServer := grpc.NewServer(connOptions...)
	rpc.RegisterWorkspaceDbServer(grpcServer, m)

	s := &Server{
		server: grpcServer,
	}

	go func() {
		s.Error = grpcServer.Serve(listener)
	}()

	return s, nil
}

// mux is the central component of the Workspace DB daemon. It receives the RPCs from
// the clients, reads/writes to the persistent database and sends notifications to
// all subscribed clients.
type mux struct {
	subscriptionLock utils.DeferableRwMutex
	// workspace name to client name
	subscriptions map[string]map[string]bool
	// client name to notification channel
	clients map[string]chan quantumfs.WorkspaceState

	// For now simply use one of the existing backends
	backend quantumfs.WorkspaceDB

	qlog *qlog.Qlog
}

func newMux(wsdb quantumfs.WorkspaceDB) *mux {
	m := mux{
		subscriptions: map[string]map[string]bool{},
		clients:       map[string]chan quantumfs.WorkspaceState{},
		backend:       wsdb,
		qlog:          qlog.NewQlogTiny(),
	}

	return &m
}

func (m *mux) newCtx(requestId uint64) *quantumfs.Ctx {
	return &quantumfs.Ctx{
		Qlog:      m.qlog,
		RequestId: requestId,
	}
}

func (m *mux) NumTypespaces(c context.Context, request *rpc.RequestId) (
	*rpc.NumTypespacesResponse, error) {

	ctx := m.newCtx(request.Id)
	num, err := m.backend.NumTypespaces(ctx)

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
		return &response, nil
	}

	return &response, err
}

func (m *mux) TypespaceTable(c context.Context, request *rpc.RequestId) (
	*rpc.TypespaceTableResponse, error) {

	ctx := m.newCtx(request.Id)
	typespaces, err := m.backend.TypespaceList(ctx)

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
		return &response, nil
	}

	return &response, err
}

func (m *mux) NumNamespaces(c context.Context, request *rpc.NamespaceRequest) (
	*rpc.NumNamespacesResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)
	num, err := m.backend.NumNamespaces(ctx, request.Typespace)

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
		return &response, nil
	}

	return &response, err
}

func (m *mux) NamespaceTable(c context.Context, request *rpc.NamespaceRequest) (
	*rpc.NamespaceTableResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)
	namespaces, err := m.backend.NamespaceList(ctx, request.Typespace)

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
		return &response, nil
	}

	return &response, err
}

func (m *mux) NumWorkspaces(c context.Context, request *rpc.WorkspaceRequest) (
	*rpc.NumWorkspacesResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)
	num, err := m.backend.NumWorkspaces(ctx, request.Typespace,
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
		return &response, nil
	}

	return &response, err
}

func (m *mux) WorkspaceTable(c context.Context, request *rpc.WorkspaceRequest) (
	*rpc.WorkspaceTableResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)
	workspaceNonces, err := m.backend.WorkspaceList(ctx, request.Typespace,
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
		return &response, nil
	}

	return &response, err
}

func (m *mux) SubscribeTo(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	clientName, ok := peer.FromContext(c)
	if !ok {
		panic("Unknown client!")
	}

	defer m.subscriptionLock.Lock().Unlock()

	m.subscriptions[request.Name][clientName.Addr.String()] = true

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       0,
		ErrCause:  "Success",
	}
	return &response, nil
}

func (m *mux) UnsubscribeFrom(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	clientName, ok := peer.FromContext(c)
	if !ok {
		panic("Unknown client!")
	}

	defer m.subscriptionLock.Lock().Unlock()

	delete(m.subscriptions, clientName.Addr.String())

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       0,
		ErrCause:  "Success",
	}
	return &response, nil
}

func (m *mux) ListenForUpdates(_ *rpc.Void,
	stream rpc.WorkspaceDb_ListenForUpdatesServer) error {

	clientName, ok := peer.FromContext(stream.Context())
	if !ok {
		panic("Unknown client!")
	}

	changes := make(chan quantumfs.WorkspaceState, 128)

	func() {
		defer m.subscriptionLock.Lock().Unlock()
		m.clients[clientName.Addr.String()] = changes
	}()

	defer func() {
		defer m.subscriptionLock.Lock().Unlock()
		delete(m.clients, clientName.Addr.String())
	}()

	for {
		select {
		case change := <-changes:
			update := rpc.WorkspaceUpdate{
				RootId: &rpc.ObjectKey{
					Data: change.RootId.Value(),
				},
				Nonce: &rpc.WorkspaceNonce{
					Nonce: uint64(change.Nonce),
				},
				Immutable: change.Immutable,
				Deleted:   change.Deleted,
			}
			err := stream.Send(&update)
			if err != nil {
				return err
			}
		}
	}

	panic("ListenForUpdates terminated unexpectedly")
	return nil
}

func (m *mux) notifyChange(workspaceName string, requestId *rpc.RequestId,
	deleted bool) {

	var update quantumfs.WorkspaceState
	if deleted {
		update.Deleted = true
	} else {
		ctx := m.newCtx(requestId.Id)

		parts := strings.Split(workspaceName, "/")
		key, nonce, err := m.backend.Workspace(ctx, parts[0], parts[1],
			parts[2])

		if err != nil {
			panic("Received error when fetching workspace")
		}

		update.RootId = key
		update.Nonce = nonce
		update.Immutable = false
		update.Deleted = false
	}

	defer m.subscriptionLock.RLock().RUnlock()
	subscriptions, ok := m.subscriptions[workspaceName]

	if !ok {
		// Nobody is subscribed to this workspace
		return
	}

	for clientName, _ := range subscriptions {
		if client, ok := m.clients[clientName]; ok {
			client <- update
		}
	}
}

func (m *mux) FetchWorkspace(c context.Context, request *rpc.WorkspaceName) (
	*rpc.FetchWorkspaceResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)

	parts := strings.Split(request.Name, "/")
	key, nonce, err := m.backend.Workspace(ctx, parts[0], parts[1], parts[2])

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
		return &response, nil
	}

	return &response, err
}

func (m *mux) BranchWorkspace(c context.Context,
	request *rpc.BranchWorkspaceRequest) (*rpc.Response, error) {

	ctx := m.newCtx(request.RequestId.Id)

	srcParts := strings.Split(request.Source, "/")
	dstParts := strings.Split(request.Destination, "/")
	err := m.backend.BranchWorkspace(ctx, srcParts[0], srcParts[1],
		srcParts[2], dstParts[0], dstParts[1], dstParts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(request.Destination, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		return &response, nil
	}
	return &response, err
}

func (m *mux) DeleteWorkspace(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	ctx := m.newCtx(request.RequestId.Id)

	parts := strings.Split(request.Name, "/")
	err := m.backend.DeleteWorkspace(ctx, parts[0], parts[1], parts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(request.Name, request.RequestId, true)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		return &response, nil
	}
	return &response, err
}

func (m *mux) SetWorkspaceImmutable(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	ctx := m.newCtx(request.RequestId.Id)

	parts := strings.Split(request.Name, "/")
	err := m.backend.SetWorkspaceImmutable(ctx, parts[0], parts[1], parts[2])

	response := rpc.Response{
		RequestId: request.RequestId,
		Err:       quantumfs.WSDB_FATAL_DB_ERROR,
		ErrCause:  "Unknown",
	}

	if err == nil {
		response.Err = 0
		response.ErrCause = "Success"
		m.notifyChange(request.Name, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Err = rpc.ResponseCodes(err.Code)
		response.ErrCause = err.Msg
		return &response, nil
	}
	return &response, err
}

func (m *mux) AdvanceWorkspace(c context.Context,
	request *rpc.AdvanceWorkspaceRequest) (*rpc.AdvanceWorkspaceResponse, error) {

	ctx := m.newCtx(request.RequestId.Id)

	parts := strings.Split(request.WorkspaceName, "/")
	currentKey := quantumfs.NewObjectKeyFromBytes(request.CurrentRootId.Data)
	newKey := quantumfs.NewObjectKeyFromBytes(request.NewRootId.Data)
	nonce := quantumfs.WorkspaceNonce(request.Nonce.Nonce)
	dbKey, err := m.backend.AdvanceWorkspace(ctx, parts[0], parts[1], parts[2],
		nonce, currentKey, newKey)

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
		m.notifyChange(request.WorkspaceName, request.RequestId, false)
		return &response, nil
	}

	if err, ok := err.(quantumfs.WorkspaceDbErr); ok {
		response.Header.Err = rpc.ResponseCodes(err.Code)
		response.Header.ErrCause = err.Msg
		return &response, nil
	}
	return &response, err
}
