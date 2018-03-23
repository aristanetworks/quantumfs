// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/testutils"
)

type testHelper struct {
	testutils.TestHelper
}

type qfsTest func(*testHelper)

func runTest(t *testing.T, test qfsTest) {
	t.Parallel()
	runTestCommon(t, test, 1500 * time.Millisecond)
}

func runTestCommon(t *testing.T, test qfsTest, timeout time.Duration) {
	// call-stack until test should be
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	th.Timeout = timeout

	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
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
