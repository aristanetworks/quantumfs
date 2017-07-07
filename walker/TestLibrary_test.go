// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

var xattrName = "user.11112222"
var xattrData = []byte("1111222233334444")

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test walkerTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

func runTestCommon(t *testing.T, test walkerTest,
	startDefaultQfs bool) {

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

func (th *testHelper) readWalkCompare(workspace string, skipDirTest bool) {

	th.SyncAllWorkspaces()

	// Restart QFS
	err := th.RestartQuantumFs()
	th.Assert(err == nil, "Error restarting QuantumFs: %v", err)
	db := th.GetWorkspaceDB()
	ds := th.GetDataStore()
	tds := newTestDataStore(th, ds)
	th.SetDataStore(tds)

	// Read all files in this workspace.
	readFile := func(path string, info os.FileInfo, inerr error) error {
		if inerr != nil {
			return inerr
		}

		if skipDirTest && info.IsDir() && strings.HasSuffix(path, "/dir1") {
			return filepath.SkipDir
		}

		if path == workspace+"/api" || info.IsDir() {
			return nil
		}

		var stat syscall.Stat_t
		var err error
		if err = syscall.Stat(path, &stat); err != nil {
			return err
		}
		if (stat.Mode & syscall.S_IFREG) == 0 {
			return nil
		}

		data := make([]byte, 100)
		sz := 0
		if sz, err = syscall.Listxattr(path, data); err != nil {
			return err
		}
		if sz != 0 {
			_, err = syscall.Getxattr(path, xattrName, data)
			if err != nil {
				return err
			}
		}

		if _, err := ioutil.ReadFile(path); err != nil {
			return err
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
	rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
	th.Assert(err == nil, "Error getting rootID for %v: %v",
		root, err)

	var walkerMap = make(map[string]int)
	var mapLock utils.DeferableMutex
	wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		// NOTE: In the TTL walker this path comparison will be
		// replaced by a TTL comparison.
		if skipDirTest && isDir && strings.HasSuffix(path, "/dir1") {
			return SkipDir
		}

		// Skip, since constant and embedded keys will not
		// show up in regular walk.
		if SkipKey(c, key) {
			return nil
		}

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
