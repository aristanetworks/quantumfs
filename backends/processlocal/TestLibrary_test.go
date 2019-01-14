// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package processlocal

import (
	"flag"
	"os"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
)

func runTest(t *testing.T, test systemlocalTest) {
	t.Parallel()

	// the stack depth of test name for all callers of runTest
	// is 1. Since the stack looks as follows:
	// 1 <testname>
	// 0 runTest
	testName := testutils.TestName(1)

	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
		wsdb: NewWorkspaceDB(""),
	}
	th.ctx = newCtx(th.Logger)

	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

type testHelper struct {
	testutils.TestHelper
	ctx  *quantumfs.Ctx
	wsdb quantumfs.WorkspaceDB
}

type systemlocalTest func(test *testHelper)

func newCtx(logger *qlog.Qlog) *quantumfs.Ctx {
	// Create  Ctx with random RequestId
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

func TestMain(m *testing.M) {
	flag.Parse()

	testutils.PreTestRuns()
	result := m.Run()
	testutils.PostTestRuns()

	os.Exit(result)
}
