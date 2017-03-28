// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "flag"
import "fmt"
import "os"
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
// Test Walk of File
// Test Walk of Dir
// Test Walk of Dir with 2 files
// Test Walk of MedFile
// Test Walk of VeryLargeFile
// Test Walk of HardLink
// Test walk of small random dir structure

type testDataStore struct {
	datastore quantumfs.DataStore
	test      *testHelper
	keys      map[quantumfs.ObjectKey]int
}

func newTestDataStore(test *testHelper, ds quantumfs.DataStore) *testDataStore {
	return &testDataStore{
		datastore: ds,
		test:      test,
		keys:      make(map[quantumfs.ObjectKey]int, 200),
	}
}

func (store *testDataStore) flushKeys() {
	store.keys = make(map[quantumfs.ObjectKey]int, 200)
}

func (store *testDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.datastore.Get(c, key, buf)
}

func (store *testDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	store.keys[key] = 1
	return store.datastore.Set(c, key, buf)
}

// Just a dummy Test. Replace with real walker related test.
func TestFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		tds := newTestDataStore(test, ds)

		test.SetDataStore(tds)

		workspace := test.NewWorkspace()
		testFilename := workspace + "/" + "testwsize"
		testFilename2 := workspace + "/" + "testwsize2"
		file, err := os.Create(testFilename)
		file2, err := os.Create(testFilename2)
		tds.flushKeys()

		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)
		defer file.Close()
		data := []byte("HowdY")
		data2 := []byte("idahsjdgsahjHowdY")
		sz, err := file.Write(data)
		_, _ = file2.Write(data2)

		test.Assert(err == nil, "Error writing to new fd: %v", err)
		test.Assert(sz == len(data), "Incorrect numbers of blocks written:",
			" expected:%d   actual:%d,  %v", len(data), sz, err)
		test.AssertLogContains("operateOnBlocks offset 0 size 5",
			"Write block size not expected")

		test.SyncAllWorkspaces()
		finfo, err := os.Stat(testFilename)
		fmt.Println("Size of the file is ", finfo.Size())
		var walkMap = make(map[quantumfs.ObjectKey]int)
		walkFunc := func(path string, key quantumfs.ObjectKey, size uint64) error {
			walkMap[key] = 1
			fmt.Println("Key while walking", path, " ", key)
			return nil
		}

		workspace = test.RelPath(workspace)
		fmt.Printf("%v\n", workspace)
		fmt.Printf("%v\n", testFilename)
		root := strings.Split(workspace, "/")
		fmt.Printf("%v\n", db)
		fmt.Printf("%v==%v==%v\n", root[0], root[1], root[2])
		rootID, err := db.Workspace(nil, root[0], root[1], root[2])
		//		rootID := th.workspaceRootId(root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		fmt.Println(rootID.String())

		err = Walk(ds, db, rootID, walkFunc)
		test.Assert(err == nil, "Error in walk: %v", err)

		for k := range tds.keys {
			fmt.Println("tds:", k)
		}

		for k := range walkMap {
			fmt.Println("walk:", k)
		}
		eq := reflect.DeepEqual(tds.keys, walkMap)
		test.Assert(eq == true, "2 maps are not equal")

		fmt.Println(walkMap)
	})
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
