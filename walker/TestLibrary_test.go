// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"flag"
	"fmt"
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
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

var xattrName = "user.11112222"
var xattrData = []byte("1111222233334444")

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

	th.walkInErrors = make([]error, 0)
	th.Timeout = 7000 * time.Millisecond
	th.CreateTestDirs()
	defer th.EndTest()

	startChan := make(chan struct{}, 0)
	th.StartDefaultQuantumFs(startChan)
	th.RunDaemonTestCommonEpilog(testName, th.testHelperUpcast(test),
		startChan, th.AbortFuse)
}

type testHelper struct {
	daemon.TestHelper
	config daemon.QuantumFsConfig

	mutex        utils.DeferableMutex
	walkInErrors []error
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
	lock      utils.DeferableMutex
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
	defer store.lock.Lock().Unlock()
	store.keys = make(map[string]int)
}

func (store *testDataStore) GetKeyList() map[string]int {
	defer store.lock.Lock().Unlock()
	return store.keys
}

func (store *testDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	defer store.lock.Lock().Unlock()
	store.keys[key.String()] = 1
	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	defer store.lock.Lock().Unlock()
	return store.datastore.Set(c, key, buf)
}

func (store *testDataStore) Freshen(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) error {

	defer store.lock.Lock().Unlock()
	return store.datastore.Freshen(c, key)
}

// checks that keys for the small files provided in hardlink paths are
// present in WSR's hardlink map
func (th *testHelper) checkSmallFileHardlinkKey(workspace string,
	hlpaths map[string]struct{}) {

	db := th.GetWorkspaceDB()
	ds := th.GetDataStore()

	// Use Walker to walk all the blocks in the workspace.
	c := &th.TestCtx().Ctx
	root := strings.Split(th.RelPath(workspace), "/")
	rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
	th.Assert(err == nil, "Error getting rootID for %v: %v",
		root, err)

	wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
		size uint64, objType quantumfs.ObjectType, err error) error {

		if err != nil {
			c.Qctx.Elog(qlog.LogTool, walkerErrLog,
				path, key.String(), err.Error())
			th.appendWalkFuncInErr(err)
			return err
		}
		// this check works for small files (1 block) only
		if _, exists := hlpaths[path]; exists {
			if !th.HardlinkKeyExists(workspace, key) {
				return fmt.Errorf(
					"Key %s Path: %s for single block "+
						"file absent in hardlink table",
					key, path)
			}
		}
		return nil
	}

	err = Walk(c, ds, rootID, wf)
	th.Assert(err == nil, "Error in walk: %v", err)
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
	err = utils.Pathwalk(workspace, readFile)
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
		size uint64, objType quantumfs.ObjectType, err error) error {

		if err != nil {
			c.Qctx.Elog(qlog.LogTool, walkerErrLog,
				path, key.String(), err.Error())
			th.appendWalkFuncInErr(err)
			return err
		}
		// NOTE: In the TTL walker this path comparison will be
		// replaced by a TTL comparison.
		if skipDirTest && objType == quantumfs.ObjectTypeDirectory &&
			strings.HasSuffix(path, "/dir1") {
			return ErrSkipDirectory
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

func (th *testHelper) appendWalkFuncInErr(err error) {
	defer th.mutex.Lock().Unlock()
	th.walkInErrors = append(th.walkInErrors, err)
}

// assertWalkFuncInErrs asserts the input error strings to walkFunc.
func (th *testHelper) assertWalkFuncInErrs(errs []string) {
	th.Assert(len(th.walkInErrors) == len(errs), "want %d errors, got %d errors",
		len(errs), len(th.walkInErrors))
	for _, e := range errs {
		found := false
		for _, w := range th.walkInErrors {
			if strings.Contains(w.Error(), e) {
				found = true
				break
			}
		}
		th.Assert(found, "substring \"%s\" not found in any errors", e)
	}
}

// assertWalkFuncQlogErrs asserts the error format strings
// expected in qlog.
func (th *testHelper) assertWalkFuncQlogErrs(errs []string) {
	th.ExpectedErrors = make(map[string]struct{})
	for _, e := range errs {
		th.ExpectedErrors["ERROR: "+e] = struct{}{}
	}
}

func (th *testHelper) nopWalkFn() WalkFunc {
	return func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
		objType quantumfs.ObjectType, err error) error {
		if err != nil {
			c.Qctx.Elog(qlog.LogTool, walkerErrLog, path, key.String(),
				err.Error())
			th.appendWalkFuncInErr(err)
			return err
		}
		return nil
	}
}

func walkWithCtx(c *quantumfs.Ctx, dsGet walkDsGet, rootID quantumfs.ObjectKey,
	wf WalkFunc) error {
	return walk(newContext(c, dsGet, rootID, wf))
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
