// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qlogstats

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

func runTest(t *testing.T, test qlogstatsTest) {
	t.Parallel()
	runTestCommon(t, test)
}

func runTestCommon(t *testing.T, test qlogstatsTest) {
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

	th.RunDaemonTestCommonEpilog(testName, th.testHelperUpcast(test),
		startChan, th.AbortFuse)
}

type testHelper struct {
	daemon.TestHelper
	config daemon.QuantumFsConfig
}

type qlogstatsTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}
