// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

// Mock test structures

import (
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends/grpc/rpc"
	"github.com/aristanetworks/quantumfs/qlog"
)

// Implements WorkspaceDb_ListenForUpdatesClient interface
type testStream struct {
	data chan *rpc.WorkspaceUpdate
}

func newTestStream() *testStream {
	return &testStream{
		data: make(chan *rpc.WorkspaceUpdate),
	}
}

func (st *testStream) Recv() (*rpc.WorkspaceUpdate, error) {
	rtn := <-st.data

	// Allow us to trigger a stream error
	if rtn == nil {
		return nil, fmt.Errorf("Nil update received")
	}

	return rtn, nil
}

func (st *testStream) Context() context.Context {
	panic("Not supported")
}

func (st *testStream) Header() (metadata.MD, error) {
	panic("Not supported")
}

func (st *testStream) Trailer() metadata.MD {
	panic("Not supported")
}

func (st *testStream) SendMsg(m interface{}) error {
	panic("Not supported")
}

func (st *testStream) RecvMsg(m interface{}) error {
	panic("Not supported")
}

func (st *testStream) CloseSend() error {
	panic("Not supported")
}

type testWorkspaceDbClient struct {
	commandDelay time.Duration
	stream       *testStream
	logger       *qlog.Qlog

	// Atomically controlled, shared by all clients in a test. When set to 1,
	// all testWorkspaceDbClients will fail requests
	serverDown *uint32
}

const (
	// Setting serverDown to this const will un-hang any function previously
	// hung by serverDownHang
	serverDownReset = iota
	// Setting serverDown to this const will cause the next request to hang,
	// upon which time serverDown will be set to serverDownReturnError
	serverDownHang
	// Setting serverDown to this const will cause all requests to return with
	// an error
	serverDownReturnError
)

func (ws *testWorkspaceDbClient) NumTypespaces(ctx context.Context,
	in *rpc.RequestId, opts ...grpc.CallOption) (*rpc.NumTypespacesResponse,
	error) {

	rtn := new(rpc.NumTypespacesResponse)
	requestId := &rpc.RequestId{
		Id: in.Id,
	}
	rtn.Header = &rpc.Response{
		RequestId: requestId,
	}

	rtn.NumTypespaces = 1

	return rtn, nil
}

func (ws *testWorkspaceDbClient) TypespaceTable(ctx context.Context,
	in *rpc.RequestId, opts ...grpc.CallOption) (*rpc.TypespaceTableResponse,
	error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) NumNamespaces(ctx context.Context,
	in *rpc.NamespaceRequest,
	opts ...grpc.CallOption) (*rpc.NumNamespacesResponse, error) {

	rtn := new(rpc.NumNamespacesResponse)
	requestId := &rpc.RequestId{
		Id: in.RequestId.Id,
	}
	rtn.Header = &rpc.Response{
		RequestId: requestId,
	}

	rtn.NumNamespaces = 1

	return rtn, nil
}

func (ws *testWorkspaceDbClient) NamespaceTable(ctx context.Context,
	in *rpc.NamespaceRequest,
	opts ...grpc.CallOption) (*rpc.NamespaceTableResponse, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) NumWorkspaces(ctx context.Context,
	in *rpc.WorkspaceRequest,
	opts ...grpc.CallOption) (*rpc.NumWorkspacesResponse, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) WorkspaceTable(ctx context.Context,
	in *rpc.WorkspaceRequest,
	opts ...grpc.CallOption) (*rpc.WorkspaceTableResponse, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) SubscribeTo(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.Response, error) {

	// do nothing
	return new(rpc.Response), nil
}

func (ws *testWorkspaceDbClient) UnsubscribeFrom(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.Response, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) ListenForUpdates(ctx context.Context, in *rpc.Void,
	opts ...grpc.CallOption) (rpc.WorkspaceDb_ListenForUpdatesClient, error) {

	return ws.stream, nil
}

func (ws *testWorkspaceDbClient) FetchWorkspace(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.FetchWorkspaceResponse,
	error) {

	serverDown := atomic.LoadUint32(ws.serverDown)
	if serverDown == serverDownHang {
		// Cause other functions to return an error instead of hanging
		atomic.StoreUint32(ws.serverDown, serverDownReturnError)

		// This function is chosen to hang indefinitely
		for {
			serverDown := atomic.LoadUint32(ws.serverDown)
			if serverDown == serverDownReset {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}
	} else if serverDown >= serverDownReturnError {
		// This function is chosen to return an error
		return nil, fmt.Errorf("Server down in test")
	}

	rtn := new(rpc.FetchWorkspaceResponse)
	requestId := &rpc.RequestId{
		Id: in.RequestId.Id,
	}
	rtn.Header = &rpc.Response{
		RequestId: requestId,
	}

	rtn.Key = &rpc.ObjectKey{
		Data: quantumfs.EmptyWorkspaceKey.Value(),
	}

	rtn.Nonce = &rpc.WorkspaceNonce{
		Id: 12345,
	}

	rtn.Immutable = false

	return rtn, nil
}

func (ws *testWorkspaceDbClient) BranchWorkspace(ctx context.Context,
	in *rpc.BranchWorkspaceRequest, opts ...grpc.CallOption) (*rpc.Response,
	error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) DeleteWorkspace(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.Response, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) SetWorkspaceImmutable(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.Response, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) AdvanceWorkspace(ctx context.Context,
	in *rpc.AdvanceWorkspaceRequest,
	opts ...grpc.CallOption) (*rpc.AdvanceWorkspaceResponse, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) sendNotification(workspace string) {
	var newNotification rpc.WorkspaceUpdate
	newNotification.Name = workspace
	newNotification.RootId = &rpc.ObjectKey{
		Data: quantumfs.EmptyWorkspaceKey.Value(),
	}
	newNotification.Nonce = &rpc.WorkspaceNonce{
		Id: 12345,
	}
	newNotification.Immutable = false

	ws.stream.data <- &newNotification
}
