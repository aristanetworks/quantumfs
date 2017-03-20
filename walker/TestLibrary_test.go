// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

// Test library for walker

import "flag"
import "fmt"
import "io/ioutil"
import "os"
import "runtime"
import "runtime/debug"
import "strings"
import "sync"
import "syscall"
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

func noStdOut(format string, args ...interface{}) error {
	// Do nothing
	return nil
}

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
	th.Init(t, testName, cachePath, qlog.NewQlogExt(cachePath+"/ramfs",
		60*10000*24, noStdOut))
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

// Global test request ID incremented for all the running tests
// This should not be visible outside daemon
var requestId = uint64(1000000000)

// Temporary directory for this test run
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

// Seperate for each package
func TestMain(m *testing.M) {
	flag.Parse()

	if os.Getuid() != 0 {
		panic("quantumfs.daemon tests must be run as root")
	}

	// Disable Garbage Collection. Because the tests provide both the filesystem
	// and the code accessing that filesystem the program is reentrant in ways
	// opaque to the golang scheduler. Thus we can end up in a deadlock situation
	// between two threads:
	//
	// ThreadFS is the filesystem, ThreadT is the test
	//
	//   ThreadFS                    ThreadT
	//                               Start filesystem syscall
	//   Start executing response
	//   <GC Wait>                   <Queue GC wait after syscal return>
	//                        DEADLOCK
	//
	// Because the filesystem request is blocked waiting on GC and the syscall
	// will never return to allow GC to progress, the test program is deadlocked.
	origGC := debug.SetGCPercent(-1)

	// Precompute a bunch of our genData to save time during tests
	//genData(40 * 1024 * 1024)

	// Setup an array for tests with errors to be logscanned later
	errorLogs = make([]logscanError, 0)

	result := m.Run()

	// We've finished running the tests and are about to do the full logscan.
	// This create a tremendous amount of garbage, so we must enable garbage
	// collection.
	runtime.GC()
	debug.SetGCPercent(origGC)

	errorMutex.Lock()
	fullLogs := make(chan string, len(errorLogs))
	var logProcessing sync.WaitGroup
	for i := 0; i < len(errorLogs); i++ {
		logProcessing.Add(1)
		go func(i int) {
			defer logProcessing.Done()
			//testSummary := outputLogError(errorLogs[i])
			//fullLogs <- testSummary
		}(i)
	}

	logProcessing.Wait()
	close(fullLogs)
	testSummary := ""
	for summary := range fullLogs {
		testSummary += summary
	}
	errorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	os.RemoveAll(daemon.TestRunDir)
	os.Exit(result)
}
