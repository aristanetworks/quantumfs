// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"fmt"
	"net"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/utils"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Start the WorkspaceDBd goroutine. This will open a socket and list on the given
// port until an error occurs.
//
// backend is a string specifying which backend to use, currently ether.cql and
// systemlocal are the only supported backends.
//
// config is the configuration string to pass to that backend.
func StartWorkspaceDbd(port uint16, backend string, config string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
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
		return err
	}

	m := mux{
		backend: wsdb,
		qlog:    qlog.NewQlogTiny(),
	}

	grpcServer := grpc.NewServer(connOptions...)
	rpc.RegisterWorkspaceDbServer(grpcServer, &m)

	return grpcServer.Serve(listener)
}

// mux is the central component of the Workspace DB daemon. It receives the RPCs from
// the clients, reads/writes to the persistent database and sends notifications to
// all subscribed clients.
type mux struct {
	subscriptionLock utils.DeferableRwMutex
	subscriptions    map[string]chan quantumfs.WorkspaceState

	// For now simply use one of the existing backends
	backend quantumfs.WorkspaceDB

	qlog *qlog.Qlog
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

	if err != nil {
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

	return &rpc.TypespaceTableResponse{}, nil
}

func (m *mux) NumNamespaces(c context.Context, request *rpc.NamespaceRequest) (
	*rpc.NumNamespacesResponse, error) {

	return &rpc.NumNamespacesResponse{}, nil
}

func (m *mux) NamespaceTable(c context.Context, request *rpc.NamespaceRequest) (
	*rpc.NamespaceTableResponse, error) {

	return &rpc.NamespaceTableResponse{}, nil
}

func (m *mux) NumWorkspaces(c context.Context, request *rpc.WorkspaceRequest) (
	*rpc.NumWorkspacesResponse, error) {

	return &rpc.NumWorkspacesResponse{}, nil
}

func (m *mux) WorkspaceTable(c context.Context, request *rpc.WorkspaceRequest) (
	*rpc.WorkspaceTableResponse, error) {

	return &rpc.WorkspaceTableResponse{}, nil
}

func (m *mux) SubscribeTo(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	return &rpc.Response{}, nil
}

func (m *mux) UnsubscribeFrom(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	return &rpc.Response{}, nil
}

func (m *mux) ListenForUpdates(*rpc.Void,
	rpc.WorkspaceDb_ListenForUpdatesServer) error {

	return nil
}

func (m *mux) FetchWorkspace(c context.Context, request *rpc.WorkspaceName) (
	*rpc.FetchWorkspaceResponse, error) {

	return &rpc.FetchWorkspaceResponse{}, nil
}

func (m *mux) BranchWorkspace(c context.Context,
	request *rpc.BranchWorkspaceRequest) (*rpc.Response, error) {

	return &rpc.Response{}, nil
}

func (m *mux) DeleteWorkspace(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	return &rpc.Response{}, nil
}

func (m *mux) SetWorkspaceImmutable(c context.Context, request *rpc.WorkspaceName) (
	*rpc.Response, error) {

	return &rpc.Response{}, nil
}

func (m *mux) AdvanceWorkspace(c context.Context,
	request *rpc.AdvanceWorkspaceRequest) (*rpc.AdvanceWorkspaceResponse, error) {

	return &rpc.AdvanceWorkspaceResponse{}, nil
}
