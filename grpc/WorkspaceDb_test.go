// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

// Test the workspaceDB implementation in grpc

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
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
	stream		*testStream
	logger		*qlog.Qlog

	// Atomically controlled, shared by all clients in a test. When set to 1,
	// all testWorkspaceDbClients will fail requests
	serverDown	*uint32
}

func (ws *testWorkspaceDbClient) NumTypespaces(ctx context.Context,
	in *rpc.RequestId, opts ...grpc.CallOption) (*rpc.NumTypespacesResponse,
	error) {

	rtn := new(rpc.NumTypespacesResponse)
	requestId := &rpc.RequestId {
		Id: 12345,
	}
	rtn.Header = &rpc.Response {
		RequestId:	requestId,
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
	requestId := &rpc.RequestId {
		Id: 12345,
	}
	rtn.Header = &rpc.Response {
		RequestId:	requestId,
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
	if serverDown == 1 {
		return nil, fmt.Errorf("Server down in test")
	}

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

func setupWsdb(test *testHelper) (quantumfs.WorkspaceDB, *quantumfs.Ctx, *uint32) {
	var serverDown uint32

	wsdb := newWorkspaceDB_("", func (*grpc.ClientConn,
		string) (*grpc.ClientConn, rpc.WorkspaceDbClient) {

		return nil, &testWorkspaceDbClient{
			commandDelay:	time.Millisecond * 1,
			logger:		test.Logger,
			stream:		newTestStream(),
			serverDown:	&serverDown,
		}
	})
	ctx := &quantumfs.Ctx {
		Qlog:	test.Logger,
	}

	return wsdb, ctx, &serverDown
}

func TestFetchWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb, ctx, _ := setupWsdb(test)

		key, nonce, err := wsdb.Workspace(ctx, "a", "b", "c")
		test.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"incorrect wsr: %s vs %s", key.String(),
			quantumfs.EmptyWorkspaceKey.String())
		test.Assert(nonce.Id == 12345, "Wrong nonce %d", nonce.Id)
		test.AssertNoErr(err)
	})
}

func TestUpdateWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb, _, _ := setupWsdb(test)

		numWorkspaces := 15000
		var workspaceLock utils.DeferableMutex
		workspaces := make(map[string]int)
		// Add some subscriptions
		for i := 0; i < numWorkspaces; i++ {
			newWorkspace := "test/test/" + strconv.Itoa(i)
			workspaces[newWorkspace] = 0
			wsdb.SubscribeTo(newWorkspace)
		}

		wsdb.SetCallback(func (updates map[string]quantumfs.WorkspaceState) {
			defer workspaceLock.Lock().Unlock()
			for wsrStr, _ := range updates {
				workspaces[wsrStr]++
			}
		})

		wsrStrs := make([]string, len(workspaces))
		for wsrStr, _ := range workspaces {
			wsrStrs = append(wsrStrs, wsrStr)
		}

		// Send in updates for every workspace
		grpcWsdb := wsdb.(*workspaceDB)
		rawWsdb := grpcWsdb.server.(*testWorkspaceDbClient)
		for _, wsrStr := range wsrStrs {
			var newNotification rpc.WorkspaceUpdate
			newNotification.Name = wsrStr
			newNotification.RootId = &rpc.ObjectKey {
				Data:	quantumfs.EmptyWorkspaceKey.Value(),
			}
			newNotification.Nonce =  &rpc.WorkspaceNonce {
				Id:	12345,
			}
			newNotification.Immutable = false

			rawWsdb.stream.data <- &newNotification
		}

		test.WaitFor("All workspaces to get notifications", func () bool {
			defer workspaceLock.Lock().Unlock()
			for _, count := range workspaces {
				if count == 0 {
					return false
				}
			}

			return true
		})
	})
}

func TestDisconnectedWorkspaceDB(t *testing.T) {
	runTestSlow(t, 10 * time.Second, func(test *testHelper) {
		
		wsdb, ctx, serverDown := setupWsdb(test)

		numWorkspaces := 100
		var workspaceLock utils.DeferableMutex
		workspaces := make(map[string]int)
		// Add some subscriptions
		for i := 0; i < numWorkspaces; i++ {
			newWorkspace := "test/test/" + strconv.Itoa(i)
			workspaces[newWorkspace] = 0

			wsdb.SubscribeTo(newWorkspace)
		}

		wsdb.SetCallback(func (updates map[string]quantumfs.WorkspaceState) {
			defer workspaceLock.Lock().Unlock()
			for wsrStr, _ := range updates {
				workspaces[wsrStr]++
			}
		})

		// Break the connection
		atomic.StoreUint32(serverDown, 1)

		grpcWsdb := wsdb.(*workspaceDB)
		rawWsdb := grpcWsdb.server.(*testWorkspaceDbClient)

		// Cause the current stream to error out, indicating connection prob
		rawWsdb.stream.data <- nil

		// Wait for many failures to happen and potentially clog things
		// when fetchWorkspace repeatedly fails
		test.WaitForNLogStrings("Received grpc error", 15000,
			"Waiting for errors to accumulate")

		// Bring the connection back
		atomic.StoreUint32(serverDown, 0)

		before := time.Now()
		// See how long it takes to do some basic operations
		_, _, err := wsdb.Workspace(ctx, "a", "b", "c")
		test.AssertNoErr(err)
		_, _, err = wsdb.Workspace(ctx, "a2", "b2", "c2")
		test.AssertNoErr(err)
		_, err = wsdb.NumTypespaces(ctx)
		test.AssertNoErr(err)
		_, err = wsdb.NumNamespaces(ctx, "test")
		test.AssertNoErr(err)
		duration := time.Since(before)
		test.Assert(duration < time.Second, "WorkspaceDB hung up")
	})
}
