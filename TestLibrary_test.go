// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "runtime"
import "strings"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

type testHelper struct {
	testutils.TestHelper
}

type qfsTest func(*testHelper)

func runTest(t *testing.T, test qfsTest) {
	t.Parallel()
	runTestCommon(t, test)
}

func runTestCommon(t *testing.T, test qfsTest) {
	testName := testutils.TestName()
	th := testutils.NewTestHelper(testName,
		testutils.TestRunDir, t)
	th.CreateTestDirs()
	defer th.EndTest()

	th.RunTestCommonEpilog(th.testHelperUpcast(test))
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
