// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
)

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test walkerTest) {
	t.Parallel()
	runTestCommon(t, test)
}

func runTestCommon(t *testing.T, test walkerTest) {

	// the stack depth of test name for all callers of runTestCommon
	// is 2. Since the stack looks as follows:
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: daemon.TestHelper{
			TestHelper: testutils.NewTestHelper(testName,
				daemon.TestRunDir, t),
		},
	}

	th.Timeout = 7000 * time.Millisecond
	th.CreateTestDirs()
	defer th.EndTest()

	startChan := make(chan struct{}, 0)
	th.StartCqlFileQuantumFs(startChan)
	th.RunDaemonTestCommonEpilog(testName, th.testHelperUpcast(test),
		startChan, th.AbortFuse)
}

type testHelper struct {
	daemon.TestHelper
	config daemon.QuantumFsConfig
}

type walkerTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

// Use the context of the actual test case.
// th.testCtx() creates a new context.
func (th *testHelper) setupWsFilesTTLs(c *Ctx,
	count int) (workspaces []string, files []string, oldTTLs []int64) {

	workspaces = make([]string, count)
	files = make([]string, count)
	oldTTLs = make([]int64, count)
	// create test workspaces
	for i := 0; i < count; i++ {
		absWorkspace := th.NewWorkspace()
		relWorkspace := th.RelPath(absWorkspace)
		parts := strings.Split(relWorkspace, "/")
		th.Assert(len(parts) == 3,
			"bad number of parts in workspace path %d %s",
			len(parts), relWorkspace)
		workspaces[i] = relWorkspace
		fpath := filepath.Join(absWorkspace, fmt.Sprintf("file-%d", i))
		files[i] = fpath

		// some content thats unique to each file
		th.AssertNoErr(testutils.PrintToFile(fpath,
			fmt.Sprintf("file-%d", i)))
		th.SyncAllWorkspaces()
		oldTTLs[i] = th.getTTL(c, fpath)
	}

	// use non-optimal walk to ensure that all keys
	// are walked irrespective of the skipmap cache.
	c.skipMap = nil
	// In legacy mode, TTLRefreshTime is saved as ms into
	// SkipMapResetAfter_ms. This test sets the threshold
	// to a large values to force a refresh.
	c.ttlCfg.SkipMapResetAfter_ms = 200000
	return workspaces, files, oldTTLs
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
