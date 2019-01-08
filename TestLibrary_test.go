// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package quantumfs

import (
	"testing"

	"github.com/aristanetworks/quantumfs/testutils"
)

type testHelper struct {
	testutils.TestHelper
}

type qfsTest func(*testHelper)

func runTest(t *testing.T, test qfsTest) {
	t.Parallel()
	runTestCommon(t, test)
}

func runTestCommon(t *testing.T, test qfsTest) {
	// call-stack until test should be
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}
