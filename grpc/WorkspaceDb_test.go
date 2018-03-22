// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

// Test the workspaceDB implementation in grpc

import (
	"fmt"
	"testing"
	"time"

	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
)

// Implements WorkspaceDb_ListenForUpdatesClient interface
type testStream struct {
	data	chan *rpc.WorkspaceUpdate
}

func newTestStream() *testStream {
	return &testStream {
		data:	make(chan *rpc.WorkspaceUpdate),
	}
}

func (st *testStream) Recv() (*rpc.WorkspaceUpdate, error) {
	rtn := <- st.data

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
	commandDelay	time.Duration
}

func (ws *testWorkspaceDbClient) NumTypespaces(ctx context.Context,
	in *rpc.RequestId, opts ...grpc.CallOption) (*rpc.NumTypespacesResponse,
	error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) TypespaceTable(ctx context.Context,
	in *rpc.RequestId, opts ...grpc.CallOption) (*rpc.TypespaceTableResponse,
	error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) NumNamespaces(ctx context.Context,
	in *rpc.NamespaceRequest,
	opts ...grpc.CallOption) (*rpc.NumNamespacesResponse, error) {

	panic("Not supported")
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

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) UnsubscribeFrom(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.Response, error) {

	panic("Not supported")
}

func (ws *testWorkspaceDbClient) ListenForUpdates(ctx context.Context, in *rpc.Void,
	opts ...grpc.CallOption) (rpc.WorkspaceDb_ListenForUpdatesClient, error) {

	return newTestStream(), nil
}

func (ws *testWorkspaceDbClient) FetchWorkspace(ctx context.Context,
	in *rpc.WorkspaceName, opts ...grpc.CallOption) (*rpc.FetchWorkspaceResponse,
	error) {

	rtn := new(rpc.FetchWorkspaceResponse)
	requestId := &rpc.RequestId {
		Id: 12345,
	}
	rtn.Header = &rpc.Response {
		RequestId:	requestId,
	}

	rtn.Key = &rpc.ObjectKey {
		Data:	quantumfs.EmptyWorkspaceKey.Value(),
	}

	rtn.Nonce = &rpc.WorkspaceNonce {
		Id:	12345,
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

func newTestWorkspaceDbClient(*grpc.ClientConn, string) (*grpc.ClientConn,
	rpc.WorkspaceDbClient) {

	return nil, &testWorkspaceDbClient{
		commandDelay:	time.Millisecond * 1,
	}
}

func TestFetchWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := newWorkspaceDB_("", newTestWorkspaceDbClient)
		ctx := &quantumfs.Ctx {
			Qlog:	test.Logger,
		}

		key, nonce, err := wsdb.Workspace(ctx, "a", "b", "c")
		test.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"incorrect wsr: %s vs %s", key.String(),
			quantumfs.EmptyWorkspaceKey.String())
		test.Assert(nonce.Id == 12345, "Wrong nonce %d", nonce.Id)
		test.AssertNoErr(err)
	})
}
