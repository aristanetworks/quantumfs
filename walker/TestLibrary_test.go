// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "flag"
import "io/ioutil"
import "os"
import "path/filepath"
import "reflect"
import "strings"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test walkerTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

func runTestCommon(t *testing.T, test walkerTest,
	startDefaultQfs bool) {

	testName := testutils.TestName()
	th := &testHelper{
		TestHelper: daemon.NewTestHelper(testName, t),
	}

	th.CreateTestDirs()
	defer th.EndTest()

	if startDefaultQfs {
		th.StartDefaultQuantumFs()
	}

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
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
	c := &th.TestCtx().Ctx
	root := strings.Split(th.RelPath(workspace), "/")
	rootID, err := db.Workspace(c, root[0], root[1], root[2])
	th.Assert(err == nil, "Error getting rootID for %v: %v",
		root, err)

	var walkerMap = make(map[string]int)
	var mapLock utils.DeferableMutex
	wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		defer mapLock.Lock().Unlock()
		walkerMap[key.String()] = 1
		return nil
	}

	err = Walk(c, ds, rootID, wf)
	th.Assert(err == nil, "Error in walk: %v", err)

	eq := reflect.DeepEqual(getMap, walkerMap)
	if eq != true {
		th.printMap("Original Map", getMap)
		th.printMap("Walker Map", walkerMap)
	}
	th.Assert(eq == true, "2 maps are not equal")
}

func (th *testHelper) readWalkCompareSkip(workspace string) {

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

		if info.IsDir() && strings.HasSuffix(path, "/dir1") {
			return filepath.SkipDir
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
	c := &th.TestCtx().Ctx
	root := strings.Split(th.RelPath(workspace), "/")
	rootID, err := db.Workspace(c, root[0], root[1], root[2])
	th.Assert(err == nil, "Error getting rootID for %v: %v",
		root, err)

	var walkerMap = make(map[string]int)
	var mapLock utils.DeferableMutex
	wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		defer mapLock.Lock().Unlock()

		// NOTE: In the TTL walker this path comparison will be
		// replaced by a TTL comparison.
		if isDir && strings.HasSuffix(path, "/dir1") {
			return SkipDir
		}

		walkerMap[key.String()] = 1
		return nil
	}

	err = Walk(c, ds, rootID, wf)
	th.Assert(err == nil, "Error in walk: %v", err)

	eq := reflect.DeepEqual(getMap, walkerMap)
	if eq != true {
		th.printMap("Original Map", getMap)
		th.printMap("Walker Map", walkerMap)
	}
	th.Assert(eq == true, "2 maps are not equal")
}

func (th *testHelper) printMap(name string, m map[string]int) {

	th.Log("%v: ", name)
	for k, v := range m {
		th.Log("%v: %v", k, v)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
