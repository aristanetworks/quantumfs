// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

// Test library for walker

//import "io/ioutil"
import "runtime"
import "strings"
import "sync"

//import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/daemon"

import "github.com/aristanetworks/quantumfs/qlog"

const fusectlPath = "/sys/fs/fuse/"

type logscanError struct {
	logFile           string
	shouldFailLogscan bool
	testName          string
}

var errorMutex sync.Mutex
var errorLogs []logscanError

// This is the normal way to run tests in the most time efficient manner
// Keep in local package
func runTest(t *testing.T, test daemon.QuantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

// Keep in local package
func runTestCommon(t *testing.T, test daemon.QuantumFsTest,
	startDefaultQfs bool) {
	// Since we grab the test name from the backtrace, it must always be an
	// identical number of frames back to the name of the test. Otherwise
	// multiple tests will end up using the same temporary directory and nothing
	// will work.
	//
	// 2 <testname>
	// 1 runTest/runExpensiveTest
	// 0 runTestCommon
	testPc, _, _, _ := runtime.Caller(2)
	testName := runtime.FuncForPC(testPc).Name()
	lastSlash := strings.LastIndex(testName, "/")
	testName = testName[lastSlash+1:]
	cachePath := daemon.TestRunDir + "/" + testName
	th := &daemon.TestHelper{}
	th.Init(t, testName, make(chan string), time.Now(), cachePath,
		qlog.NewQlogExt(cachePath+"/ramfs", 60*10000*24, daemon.NoStdOut))
	/*
		th := &daemon.TestHelper{
			t:          t,
			testName:   testName,
			testResult: make(chan string),
			startTime:  time.Now(),
			cachePath:  cachePath,
		}
	*/

	th.CreateTestDirs()

	defer th.EndTest()

	// Allow tests to run for up to 1 seconds before considering them timed out.
	// If we are going to start a standard QuantumFS instance we can start the
	// timer before the test proper and therefore avoid false positive test
	// failures due to timeouts caused by system slowness as we try to mount
	// dozens of FUSE filesystems at once.
	if startDefaultQfs {
		th.StartDefaultQuantumFs()
	}

	th.Log("Finished test preamble, starting test proper")
	go th.Execute(test)

	var testResult string

	select {
	case <-time.After(10000 * time.Millisecond):
		testResult = "ERROR: TIMED OUT"

	case testResult = <-th.TestResult:
	}

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
	}
}

// Global test request ID incremented for all the running tests
// This should not be visible outside daemon
var requestId = uint64(1000000000)

// Temporary directory for this test run
/*
func init() {
	syscall.Umask(0)

	var err error
	for i := 0; i < 10; i++ {
		daemon.TestRunDir, err = ioutil.TempDir("", "quantumfsTest")
		if err != nil {
			continue
		}
		if err := os.Chmod(daemon.TestRunDir, 777); err != nil {
			continue
		}
		return
	}
	panic(fmt.Sprintf("Unable to create temporary test directory: %v", err))
}
*/
