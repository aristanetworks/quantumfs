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

// common test codes for best-effort and fail-fast modes.

func doPanicStringTest(bestEffort bool) func(*testHelper) {

	return func(test *testHelper) {
		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedString := "raised panic"
		expectedErr := fmt.Errorf(expectedString)

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path, key.String(),
					err.Error())
				test.appendWalkFuncInErr(err)
				return err
			}

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedString)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(), expectedErr.Error()),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		test.assertWalkFuncInErrs([]string{expectedString})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doPanicErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErrString := "raised panic"
		expectedErr := fmt.Errorf(expectedErrString)

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path, key.String(),
					err.Error())
				test.appendWalkFuncInErr(err)
				return err
			}

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedErr)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(),
			expectedErr.Error()),
			"Walk did not get the expectedErr value, instead got %v",
			err)
		test.assertWalkFuncInErrs([]string{expectedErr.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doWalkErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErr := fmt.Errorf("send error")

		// Write File 1
		filename := workspace + "/errorFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path, key.String(),
					err.Error())
				test.appendWalkFuncInErr(err)
				return err
			}

			if strings.HasSuffix(path, "/errorFile") {
				return expectedErr
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		// since errors generated in walkFunc aren't reflected back into
		// walkFunc.
		test.assertWalkFuncInErrs(nil)
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doHLGetErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create files in the workspace
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, i)
			data := daemon.GenData(1)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// setup hardlinks so that more than one HLE blocks
		// are used.
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			link := fmt.Sprintf("%s/link-%d", workspace, i)
			fname := fmt.Sprintf("%s/file-%d", workspace, i)
			err := os.Link(fname, link)
			test.Assert(err == nil, "Link failed (%s): %s",
				link, err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		hleGetError := fmt.Errorf("hardlinkEntry error")
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeHardlink {
				return hleGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == hleGetError.Error(),
			"Walk did not get the %v, instead got %v", hleGetError,
			err)
		test.assertWalkFuncInErrs([]string{hleGetError.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doDEGetErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create directories in the workspace
		dirCount := 3
		for i := 0; i < dirCount; i++ {
			dirName := fmt.Sprintf("%s/dir-%d", workspace, i)
			err := os.MkdirAll(dirName, 0666)
			test.Assert(err == nil, "MkdirAll failed (%s): %s",
				dirName, err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		deGetError := fmt.Errorf("directoryEntry error")
		// setup dsGetHelper return error upon Get of
		// HardLinkEntry. This is the second HardLinkEntry
		// since first one is embedded inside WSR.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeDirectory &&
				path == "/dir-1" {
				return deGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == deGetError.Error(),
			"Walk did not get the %v, instead got %v", deGetError,
			err)
		test.assertWalkFuncInErrs([]string{deGetError.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doEAGetErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create files in the workspace
		files := 2
		for f := 0; f < files; f++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, f)
			data := daemon.GenData(1)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// add extattr to the first file (workspace/file-0)
		err := syscall.Setxattr(fmt.Sprintf("%s/file-0",
			workspace), xattrName, xattrData, 0)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		eaGetError := fmt.Errorf("extended attributes error")
		// setup dsGetHelper return error upon Get of
		// extattr of file-0.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeExtendedAttribute &&
				path == "/file-0" {
				return eaGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == eaGetError.Error(),
			"Walk did not get the %v, instead got %v", eaGetError,
			err)
		test.assertWalkFuncInErrs([]string{eaGetError.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doEAAttrGetErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create large files in the workspace.
		// use large files to be able to distinguish these from
		// extattr data which is of type ObjectTypeSmallFile
		files := 2
		for f := 0; f < files; f++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, f)
			data := daemon.GenData(1024 * 1024 * 33)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// add extattrs to the first file (workspace/file-0)
		testXattrNameFmt := "extattr-%d"
		attrs := 10
		for a := 0; a < attrs; a++ {
			err := syscall.Setxattr(fmt.Sprintf("%s/file-0", workspace),
				fmt.Sprintf(testXattrNameFmt, a),
				daemon.GenData(10), 0)
			test.AssertNoErr(err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		eaGetError := fmt.Errorf("extended attributes error")
		// setup dsGetHelper to return error upon Get of
		// second extattr on file-0.
		countAttrGet := 0
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeExtendedAttribute &&
				path == "/file-0" {
				countAttrGet++
				if countAttrGet == 2 {
					return eaGetError
				}
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == eaGetError.Error(),
			"Walk did not get the %v, instead got %v", eaGetError,
			err)
		test.assertWalkFuncInErrs([]string{eaGetError.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doMultiBlockGetErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a large file and small file in the workspace.
		largeFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1024 * 1024 * 33)
		err := ioutil.WriteFile(largeFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			largeFname, err)

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		mbGetBlock0Error := fmt.Errorf("multiblock error")
		// setup dsGetHelper return error upon Get of multiblock
		// buffer on file-0. Since large file has 1 multiblock
		// metadata block, failing to get that causes other
		// data blocks in that file to be skipped.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeLargeFile &&
				path == "/file-0" {
				return mbGetBlock0Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == mbGetBlock0Error.Error(),
			"Walk did not get the %v, instead got %v", mbGetBlock0Error,
			err)
		test.assertWalkFuncInErrs([]string{mbGetBlock0Error.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func doVLFileGetFirstErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a small file
		vlFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1)
		err := ioutil.WriteFile(vlFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			vlFname, err)
		// now convert the small file to a very large file

		os.Truncate(vlFname,
			int64(int(quantumfs.MaxLargeFileSize())+
				quantumfs.MaxBlockSize))

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		vlGetBlock0Error := fmt.Errorf("verylarge block0 error")
		// setup dsGetHelper return error upon Get of block0
		// (multiblock metadata block) on file-0.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeVeryLargeFile &&
				path == "/file-0" {
				return vlGetBlock0Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == vlGetBlock0Error.Error(),
			"Walk did not get the %v, instead got %v", vlGetBlock0Error,
			err)
		test.assertWalkFuncInErrs([]string{vlGetBlock0Error.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})

	}
}

func doVLFileGetNextErrTest(bestEffort bool) func(*testHelper) {
	return func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a small file
		vlFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1)
		err := ioutil.WriteFile(vlFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			vlFname, err)
		// now convert the small file to a very large file

		os.Truncate(vlFname,
			int64(int(quantumfs.MaxLargeFileSize())+
				quantumfs.MaxBlockSize))

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		vlGetBlock1Error := fmt.Errorf("verylarge block1 error")
		// setup dsGetHelper return error upon Get of block1
		// (second level multiblock metadata block) on file-0.
		seenVLFblock := false
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeVeryLargeFile &&
				path == "/file-0" {
				seenVLFblock = true
			}

			if typ == quantumfs.ObjectTypeLargeFile &&
				path == "/file-0" &&
				seenVLFblock {
				seenVLFblock = false
				return vlGetBlock1Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == vlGetBlock1Error.Error(),
			"Walk did not get the %v, instead got %v", vlGetBlock1Error,
			err)
		test.assertWalkFuncInErrs([]string{vlGetBlock1Error.Error()})
		test.assertWalkFuncQlogErrs([]string{walkerErrLog})
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
