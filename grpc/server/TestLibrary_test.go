// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/grpc"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

var serversLock utils.DeferableMutex
var servers map[uint16]*Server

func init() {
	servers = map[uint16]*Server{}
}

const initialPort = uint16(22222)

func runTest(t *testing.T, test serverTest) {
	t.Parallel()

	// the stack depth of test name for all callers of runTest
	// is 1. Since the stack looks as follows:
	// 1 <testname>
	// 0 runTest
	testName := testutils.TestName(1)

	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	th.ctx = newCtx(th.Logger)

	func() {
		defer serversLock.Lock().Unlock()
		port := initialPort
		for {
			if _, used := servers[port]; !used {
				break
			}
			port++
		}

		server, err := StartWorkspaceDbd(th.Logger, port, "processlocal", "")
		if err != nil {
			t.Fatalf(fmt.Sprintf("Failed to initialize wsdb server: %s",
				err.Error()))
		}

		servers[port] = server
		th.server = server
		th.port = port
	}()

	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

type testHelper struct {
	testutils.TestHelper
	ctx    *quantumfs.Ctx
	server *Server
	port   uint16
}

type serverTest func(test *testHelper)

func newCtx(logger *qlog.Qlog) *quantumfs.Ctx {
	// Create Ctx with random RequestId
	Qlog := logger
	requestId := qlog.TestReqId
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestId,
	}

	return ctx
}

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

func (th *testHelper) EndTest() {
	if th.server != nil {
		err := th.server.Stop()
		if err != nil && !strings.Contains(err.Error(),
			"use of closed network connection") {

			th.AssertNoErr(err)
		}
	}

	func() {
		defer serversLock.Lock().Unlock()
		delete(servers, th.port)
	}()

	th.TestHelper.EndTest()
}

func TestMain(m *testing.M) {
	flag.Parse()

	testutils.PreTestRuns()
	result := m.Run()
	testutils.PostTestRuns()

	os.Exit(result)
}

func (th *testHelper) newClient() quantumfs.WorkspaceDB {
	config := fmt.Sprintf("[::1]:%d", th.port)
	client := grpc.NewWorkspaceDB(config)

	return client
}

func (th *testHelper) restartServer() {
	th.server.Stop()
	th.server = nil

	defer serversLock.Lock().Unlock()
	server, err := StartWorkspaceDbd(th.Logger, th.port, "processlocal", "")
	th.AssertNoErr(err)

	th.server = server
	servers[th.port] = th.server
}
