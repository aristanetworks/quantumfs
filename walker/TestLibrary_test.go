// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "flag"
import "io/ioutil"
import "os"
import "path/filepath"
import "reflect"
import "runtime"
import "strings"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test walkerTest) {
	t.Parallel()
	runTestCommon(t, test, true)
	//daemon.RunTestCommon(t, th.testHelperUpcast(test))
}

func runTestCommon(t *testing.T, test walkerTest,
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
	config daemon.QuantumFsConfig
}

type walkerTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

type testDataStore struct {
	datastore quantumfs.DataStore
	test      *testHelper
	keys      map[string]int
}

func newTestDataStore(test *testHelper, ds quantumfs.DataStore) *testDataStore {
	return &testDataStore{
		datastore: ds,
		test:      test,
		keys:      make(map[string]int),
	}
}

func (store *testDataStore) FlushKeyList() {
	store.keys = make(map[string]int)
}

func (store *testDataStore) GetKeyList() map[string]int {
	return store.keys
}

func (store *testDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	store.keys[key.String()] = 1
	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.datastore.Set(c, key, buf)
}

func (th *testHelper) readWalkCompare(workspace string) {

	th.SyncAllWorkspaces()

	// Restart QFS
	err := th.RestartQuantumFs()
	th.Assert(err == nil, "Error restarting QuantumFs: %v", err)
	db := th.GetWorkspaceDB()
	ds := th.GetDataStore()
	tds := newTestDataStore(th, ds)
	th.SetDataStore(tds)

	// Read all files in this workspace.
	readFile := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if !info.IsDir() {
			ioutil.ReadFile(path)
		}
		return nil
	}
	err = filepath.Walk(workspace, readFile)
	th.Assert(err == nil, "Normal walk failed (%s): %s", workspace, err)

	// Save the keys intercepted during filePath walk.
	getMap := tds.GetKeyList()
	tds.FlushKeyList()

	// Use Walker to walk all the blocks in the workspace.
	root := strings.Split(th.RelPath(workspace), "/")
	rootID, err := db.Workspace(newCtx(), root[0], root[1], root[2])
	th.Assert(err == nil, "Error getting rootID for %v: %v",
		root, err)

	var walkerMap = make(map[string]int)
	walkFunc := func(path string, key quantumfs.ObjectKey, size uint64) error {
		walkerMap[key.String()] = 1
		return nil
	}

	err = Walk(ds, rootID, walkFunc)
	th.Assert(err == nil, "Error in walk: %v", err)

	eq := reflect.DeepEqual(getMap, walkerMap)
	th.Assert(eq == true, "2 maps are not equal")
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
	requestId := qlog.TestReqId
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestId,
	}

	return ctx
}
