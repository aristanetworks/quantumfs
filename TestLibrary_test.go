// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
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
	// Since we grab the test name from the backtrace, it must always be an
	// identical number of frames back to the name of the test. Otherwise
	// multiple tests will end up using the same temporary directory and nothing
	// will work.
	//
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testPc, _, _, _ := runtime.Caller(2)
	testName := runtime.FuncForPC(testPc).Name()
	lastSlash := strings.LastIndex(testName, "/")
	testName = testName[lastSlash+1:]
	cachePath := testutils.TestRunDir + "/" + testName

	noStdOut := func(format string, args ...interface{}) error {
		return nil
	}

	th := &testHelper{
		testutils.TestHelper{
			T:          t,
			TestName:   testName,
			TestResult: make(chan string, 2), // must be buffered
			StartTime:  time.Now(),
			CachePath:  cachePath,
			Logger: qlog.NewQlogExt(cachePath+"/ramfs",
				60*10000*24, noStdOut),
		},
	}

	th.CreateTestDirs()
	defer th.EndTest()

	th.Log("Finished test preamble, starting test proper")
	go th.Execute(th.testHelperUpcast(test))

	testResult := th.WaitForResult()

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
	}
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
