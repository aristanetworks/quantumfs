// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

// Test the workspaceDB implementation in grpc

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	grpc "google.golang.org/grpc"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/utils"
)

func setupWsdb(test *testHelper) (quantumfs.WorkspaceDB, *quantumfs.Ctx, *uint32) {
	var serverDown uint32

	wsdb := newWorkspaceDB_("", func(*grpc.ClientConn,
		string) (*grpc.ClientConn, rpc.WorkspaceDbClient) {

		return nil, &testWorkspaceDbClient{
			commandDelay: time.Millisecond * 1,
			logger:       test.Logger,
			stream:       newTestStream(),
			serverDown:   &serverDown,
		}
	})
	ctx := &quantumfs.Ctx{
		Qlog: test.Logger,
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

		wsdb.SetCallback(func(updates map[string]quantumfs.WorkspaceState) {
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
			newNotification.RootId = &rpc.ObjectKey{
				Data: quantumfs.EmptyWorkspaceKey.Value(),
			}
			newNotification.Nonce = &rpc.WorkspaceNonce{
				Id: 12345,
			}
			newNotification.Immutable = false

			rawWsdb.stream.data <- &newNotification
		}

		test.WaitFor("All workspaces to get notifications", func() bool {
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
	runTest(t, func(test *testHelper) {

		wsdb, ctx, serverDown := setupWsdb(test)

		numWorkspaces := 1000
		var workspaceLock utils.DeferableMutex
		workspaces := make(map[string]int)
		// Add some subscriptions
		for i := 0; i < numWorkspaces; i++ {
			newWorkspace := "test/test/" + strconv.Itoa(i)
			workspaces[newWorkspace] = 0

			wsdb.SubscribeTo(newWorkspace)
		}

		wsdb.SetCallback(func(updates map[string]quantumfs.WorkspaceState) {
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

		// Queue up >1000 commands which should now fail with errors
		for i := 0; i < 1100; i++ {
			go func() {
				defer func() {
					// suppress any panics due to reconnect()
					recover()
				}()

				_, _, err := wsdb.Workspace(ctx, "a", "b", "c")
				test.AssertErr(err)
			}()
		}

		// Wait for many failures to happen and potentially clog things
		// when fetchWorkspace repeatedly fails
		test.WaitForNLogStrings("Received grpc error", 1000,
			"fetch errors to accumulate")

		// Bring the connection back
		atomic.StoreUint32(serverDown, 0)

		// Perform some basic operations which need the wsdb.lock -
		// if the lock is hung, then this test will timeout
		wsdb.SubscribeTo("post/test/wsrA")
		wsdb.SubscribeTo("post/test/wsrB")
		wsdb.SubscribeTo("post/test/wsrC")
	})
}
