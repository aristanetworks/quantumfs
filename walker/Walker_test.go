// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

//import "bytes"
import "flag"
import "fmt"
import "io/ioutil"
import "os"
import "path/filepath"

import "reflect"
import "runtime"
import "strings"

//import "strconv"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs"
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
	config daemon.QuantumFsConfig
}

type quantumFsTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

// TODO(sid)
// Test Walk of File : Works
// Test Walk of Dir : Works
// Test Walk of Dir with file: Works
// Test Walk of Dir with 2 files: Works
// Test Walk of MedFile
// Test Walk of VeryLargeFile
// Test Walk of HardLink
// Test walk of small random dir structure

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

func (store *testDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	store.keys[key.String()] = 1
	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.datastore.Set(c, key, buf)
}

func TestFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		filename := workspace + "/file"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		filename = dirname + "/file"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		filename2 := dirname + "/file2"
		err = ioutil.WriteFile(filename2, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename2, err)

		link := workspace + "/filelink"
		err = os.Link(filename2, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		// Restart
		err = test.RestartQuantumFs()
		test.Assert(err == nil, "Error restarting QuantumFs: %v", err)
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		tds := newTestDataStore(test, ds)
		test.SetDataStore(tds)

		// Read
		readFile := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Println(err)
				return nil
			}
			fmt.Println(path)

			if !info.IsDir() {
				ioutil.ReadFile(path)
			}
			return nil
		}
		err = filepath.Walk(workspace, readFile)
		test.Assert(err == nil, "Normal walk failed (%s): %s", workspace, err)

		// Use Walker
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, err := db.Workspace(nil, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		var walkMap = make(map[string]int)
		walkFunc := func(path string, key quantumfs.ObjectKey, size uint64) error {
			fmt.Println(path, ": ", key)
			walkMap[key.String()] = 1
			return nil
		}

		err = Walk(ds, db, rootID, walkFunc)
		test.Assert(err == nil, "Error in walk: %v", err)

		var i int = 1
		for k := range tds.keys {
			fmt.Println("tds:", k, " ", i)
			i++
		}

		i = 1
		for k := range walkMap {
			fmt.Println("walk:", k, " ", i)
			i++
		}
		eq := reflect.DeepEqual(tds.keys, walkMap)
		test.Assert(eq == true, "2 maps are not equal")
	})
}

/*
func TestFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(5)
		workspace := test.NewWorkspace()
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		err = test.RestartQuantumFs()
		test.Assert(err == nil, "Error restarting QuantumFs: %v", err)
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		tds := newTestDataStore(test, ds)
		test.SetDataStore(tds)

		fileData, err := ioutil.ReadFile(filename)
		test.Assert(err == nil, "File lost (%s): %s",
			filename, err)
		test.Assert(bytes.Equal(fileData, data),
			"File data doesn't match in %s", filename)

		var walkMap = make(map[string]int)
		walkFunc := func(path string, key quantumfs.ObjectKey, size uint64) error {
			walkMap[key.String()] = 1
			return nil
		}

		workspaceRel := test.RelPath(workspace)
		root := strings.Split(workspaceRel, "/")
		rootID, err := db.Workspace(nil, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		err = Walk(ds, db, rootID, walkFunc)
		test.Assert(err == nil, "Error in walk: %v", err)

		for k, val := range tds.keys {
			fmt.Println("tds:", k, " ", val)
		}

		for k, val := range walkMap {
			fmt.Println("walk:", k, " ", val)
		}
		eq := reflect.DeepEqual(tds.keys, walkMap)
		test.Assert(eq == true, "2 maps are not equal")
	})
}
*/
func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
