// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "flag"
import "os"
import "runtime"
import "strings"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

func runTestCommon(t *testing.T, test quantumFsTest,
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

	th := &testHelper{
		TestHelper: daemon.TestHelper{
			TestHelper: testutils.TestHelper{
				T:          t,
				TestName:   testName,
				TestResult: make(chan string, 2), // must be buffered
				StartTime:  time.Now(),
				CachePath:  cachePath,
				Logger: qlog.NewQlogExt(cachePath+"/ramfs",
					60*10000*24, daemon.NoStdOut),
			},
		},
	}

	th.CreateTestDirs()
	defer th.EndTest()

	if startDefaultQfs {
		th.StartDefaultQuantumFs()
	}

	th.Log("Finished test preamble, starting test proper")
	go th.Execute(th.testHelperUpcast(test))

	testResult := th.WaitForResult()

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
	}
}

type testHelper struct {
	daemon.TestHelper
}

type quantumFsTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

// TODO(sid)
// Test Walk of Dir
// Test Walk of File
// Test Walk of MedFile
// Test Walk of VeryLargeFile
// Test Walk of HardLink
// Test walk of small random dir structure

// Just a dummy Test. Replace with real walker related test.
func TestFileWriteBlockSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/" + "testwsize"
		file, err := os.Create(testFilename)
		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)
		defer file.Close()

		data := []byte("HowdY")

		sz, err := file.Write(data)
		test.Assert(err == nil, "Error writing to new fd: %v", err)
		test.Assert(sz == len(data), "Incorrect numbers of blocks written:",
			" expected:%d   actual:%d,  %v", len(data), sz, err)
		test.AssertLogContains("operateOnBlocks offset 0 size 5",
			"Write block size not expected")
	})
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
