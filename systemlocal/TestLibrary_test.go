// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

import "flag"
import "os"
import "fmt"
import "io/ioutil"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"

func runTest(t *testing.T, test systemlocalTest) {
	t.Parallel()

	// the stack depth of test name for all callers of runTest
	// is 1. Since the stack looks as follows:
	// 1 <testname>
	// 0 runTest
	testName := testutils.TestName(1)
	testDir, err := ioutil.TempDir("", testName)
	if err != nil {
		panic(fmt.Sprintf("Unable to create test directory: %v", err))
	}
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
		path: testDir,
		db:   NewWorkspaceDB(testDir + "/db"),
	}

	th.CreateTestDirs()
	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

type testHelper struct {
	testutils.TestHelper
	db   quantumfs.WorkspaceDB
	path string
}

type systemlocalTest func(test *testHelper)

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
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
