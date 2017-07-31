// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"fmt"
	"net"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/utils"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Start the WorkspaceDBd goroutine. This will open a socket and list on the given
// port until an error occurs.
func StartWorkspaceDbd(port uint16) error {
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

	grpcServer := grpc.NewServer(connOptions...)
	rpc.RegisterWorkspaceDbServer(grpcServer, &mux{})

	return grpcServer.Serve(listener)
}

// mux is the central component of the Workspace DB daemon. It receives the RPCs from
// the clients, reads/writes to the persistent database and sends notifications to
// all subscribed clients.
type mux struct {
	subscriptionLock utils.DeferableRwMutex
	subscriptions    map[string]chan quantumfs.WorkspaceState
}

func (m *mux) NumTypespaces(c context.Context, request *rpc.RequestId) (
	*rpc.NumTypespacesResponse, error) {

	return &rpc.NumTypespacesResponse{}, nil
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
