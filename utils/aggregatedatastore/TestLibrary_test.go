// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package aggregatedatastore

import (
	"sync/atomic"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends/processlocal"
	"github.com/aristanetworks/quantumfs/testutils"
)

type testHelper struct {
	testutils.TestHelper
	ds quantumfs.DataStore
	c  *quantumfs.Ctx
}

type adsTest func(*testHelper)

func runTest(t *testing.T, test adsTest) {
	// call-stack until test should be
	// 1 <testname>
	// 0 runTest
	testName := testutils.TestName(1)
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	defer th.EndTest()

	th.ds = processlocal.NewDataStore("")
	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

func (th *testHelper) testCtx() *quantumfs.Ctx {
	log := th.Logger
	reqId := atomic.AddUint64(&requestId, 1)
	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: reqId,
	}
	return c
}
