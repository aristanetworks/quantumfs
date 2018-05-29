// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

import (
	"flag"
	"os"
	"testing"

	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
)

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}

func runTest(t *testing.T, test qfsclientTest) {
	t.Parallel()
	runTestCommon(t, test)
}

func runTestCommon(t *testing.T, test qfsclientTest) {
	// the stack depth of test name for all callers of runTestCommon
	// is 2. Since the stack looks as follows:
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: daemon.TestHelper{
			TestHelper: testutils.NewTestHelper(testName,
				daemon.TestRunDir, t),
		},
	}

	th.CreateTestDirs()
	defer th.EndTest()

	startChan := make(chan struct{}, 0)

	th.StartDefaultQuantumFs(startChan)
	th.waitForApi()

	th.RunDaemonTestCommonEpilog(testName, th.testHelperUpcast(test),
		startChan, th.AbortFuse)
}

type testHelper struct {
	daemon.TestHelper
	config daemon.QuantumFsConfig
}

type qlogTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

func (th *testHelper) waitForApi() {
	th.WaitFor("Api inode to be seen by kernel", func() bool {
		_, err := os.Stat(th.TempDir + "/mnt/api")
		return (err == nil)
	})
}

func (th *testHelper) getApi() QfsClientApi {
	api, err := GetApiPath(th.TempDir + "/mnt/api")
	th.AssertNoErr(err)
	return api
}

func (th *testHelper) putApi(api QfsClientApi) {
	th.AssertNoErr(ReleaseApi(api))
}
