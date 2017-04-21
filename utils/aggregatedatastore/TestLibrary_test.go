// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package aggregatedatastore

import "sync/atomic"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/testutils"

type testHelper struct {
	testutils.TestHelper
	ds quantumfs.DataStore
	c  *quantumfs.Ctx
}

type adsTest func(*testHelper)

func runTest(t *testing.T, test adsTest) {
	// call-stack until test should be
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	th.CreateTestDirs()
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

func (th *testHelper) CreateTestDirs() {
	th.TempDir = testutils.TestRunDir + "/" + th.TestName
	utils.MkdirAll(th.TempDir, 0777)
	th.Log("Using TestDir %s", th.TempDir)
}

func (th *testHelper) testCtx() *quantumfs.Ctx {
	log := qlog.NewQlogTiny()
	reqId := atomic.AddUint64(&requestId, 1)
	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: reqId,
	}
	return c
}
