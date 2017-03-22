// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

// Test library for walker
import "runtime"
import "strings"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/qlog"

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test daemon.QuantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

func runTestCommon(t *testing.T, test daemon.QuantumFsTest,
	startDefaultQfs bool) {
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
	cachePath := daemon.TestRunDir + "/" + testName
	th := &daemon.TestHelper{}
	th.Init(t, testName, make(chan string), time.Now(), cachePath,
		qlog.NewQlogExt(cachePath+"/ramfs", 60*10000*24, daemon.NoStdOut))

	th.CreateTestDirs()

	defer th.EndTest()

	if startDefaultQfs {
		th.StartDefaultQuantumFs()
	}

	th.Log("Finished test preamble, starting test proper")
	go th.Execute(test)

	var testResult string

	// Allow tests to run for up to 1 seconds before considering them timed out.
	select {
	case <-time.After(1000 * time.Millisecond):
		testResult = "ERROR: TIMED OUT"

	case testResult = <-th.TestResult:
	}

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
	}
}
