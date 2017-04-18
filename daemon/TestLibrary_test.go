// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"
import "flag"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "reflect"
import "strings"
import "sync/atomic"
import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"

func TestMain(m *testing.M) {
	flag.Parse()

	// Precompute a bunch of our GenData to save time during tests
	GenData(40 * 1024 * 1024)

	PreTestRuns()
	result := m.Run()
	PostTestRuns()

	os.Exit(result)
}

func TestRandomNamespaceName(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		name1 := randomNamespaceName(8)
		name2 := randomNamespaceName(8)
		name3 := randomNamespaceName(10)

		test.Assert(len(name1) == 8, "name1 wrong length: %d", len(name1))
		test.Assert(name1 != name2, "name1 == name2: '%s'", name1)
		test.Assert(len(name3) == 10, "name3 wrong length: %d", len(name1))
	})
}

// If a test never returns from some event, such as an inifinite loop, the test
// should timeout and cleanup after itself.
func TestTimeout(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ShouldFail = true
		time.Sleep(60 * time.Second)

		// If we get here then the test library didn't time us out and we
		// sould fail this test.
		test.ShouldFail = false
		test.Assert(false, "Test didn't fail due to timeout")
	})
}

func TestGenData(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		hardcoded := "012345678910111213141516171819202122232425262"
		data := GenData(len(hardcoded))

		test.Assert(bytes.Equal([]byte(hardcoded), data),
			"Data gen function off: %s vs %s", hardcoded, data)
	})
}

// If a quantumfs test fails then it may leave the filesystem mount hanging around in
// a blocked state. testHelper needs to forcefully abort and umount these to keep the
// system functional. Test this forceful unmounting here.
func TestPanicFilesystemAbort(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ShouldFailLogscan = true

		api := test.getApi()

		// Introduce a panicing error into quantumfs
		test.qfs.mapMutex.Lock()
		for k, v := range test.qfs.fileHandles {
			test.qfs.fileHandles[k] = &crashOnWrite{FileHandle: v}
		}
		test.qfs.mapMutex.Unlock()

		// panic Quantumfs
		api.Branch(quantumfs.NullSpaceName+"/"+quantumfs.NullSpaceName+"/"+
			quantumfs.NullSpaceName, "branch/test/crash")
	})
}

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true, nil)
}

// If you need to initialize the QuantumFS instance in some special way,
// then use this variant.
func runTestNoQfs(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, false, nil)
}

// configModifier is a function which is given the default configuration
// and should make whichever modifications the test requires in place.
type configModifierFunc func(test *testHelper, config *QuantumFsConfig)

// If you need to initialize QuantumFS with a special configuration, but not poke
// into its internals before the test proper begins, use this.
func runTestCustomConfig(t *testing.T, configModifier configModifierFunc,
	test quantumFsTest) {

	t.Parallel()
	runTestCommon(t, test, true, configModifier)
}

// If you need to initialize the QuantumFS instance in some special way and the test
// is relatively expensive, then use this variant.
func runTestNoQfsExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, false, nil)
}

// If you have a test which is expensive in terms of CPU time, then use
// runExpensiveTest() which will not run it at the same time as other tests. This is
// to prevent multiple expensive tests from running concurrently and causing each
// other to time out due to CPU starvation.
func runExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, true, nil)
}

func runTestCommon(t *testing.T, test quantumFsTest, startDefaultQfs bool,
	configModifier configModifierFunc) {

	testName := testutils.TestName()
	th := &testHelper{
		TestHelper: TestHelper{
			TestHelper: testutils.NewTestHelper(testName,
				TestRunDir, t),
		},
	}
	th.CreateTestDirs()
	defer th.EndTest()

	// Allow tests to run for up to 1 seconds before considering them timed out.
	// If we are going to start a standard QuantumFS instance we can start the
	// timer before the test proper and therefore avoid false positive test
	// failures due to timeouts caused by system slowness as we try to mount
	// dozens of FUSE filesystems at once.
	if startDefaultQfs {
		config := th.defaultConfig()
		if configModifier != nil {
			configModifier(th, &config)
		}

		th.startQuantumFs(config)
	}

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

type quantumFsTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

// testHelper holds the variables important to maintain the state of testing
// in a package. This helper is more of a namespacing mechanism than a
// coherent object.
type testHelper struct {
	TestHelper
}

// Retrieve a list of FileDescriptor from an Inode
func (th *testHelper) fileDescriptorFromInodeNum(inodeNum uint64) []*FileDescriptor {
	handles := make([]*FileDescriptor, 0)

	defer th.qfs.mapMutex.Lock().Unlock()

	for _, file := range th.qfs.fileHandles {
		fh, ok := file.(*FileDescriptor)
		if !ok {
			continue
		}

		if fh.inodeNum == InodeId(inodeNum) {
			handles = append(handles, fh)
		}
	}

	return handles
}

// Return the inode number from QuantumFS. Fails if the absolute path doesn't exist.
func (th *testHelper) getInodeNum(path string) InodeId {
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	th.Assert(err == nil, "Error grabbing file inode (%s): %v", path, err)

	return InodeId(stat.Ino)
}

// Retrieve the Inode from Quantumfs. Returns nil is not instantiated
func (th *testHelper) getInode(path string) Inode {
	inodeNum := th.getInodeNum(path)
	return th.qfs.inodeNoInstantiate(&th.qfs.c, inodeNum)
}

func (th *testHelper) workspaceRootId(typespace string, namespace string,
	workspace string) quantumfs.ObjectKey {

	key, err := th.qfs.c.workspaceDB.Workspace(&th.newCtx().Ctx,
		typespace, namespace, workspace)
	th.Assert(err == nil, "Error fetching key")

	return key
}

// Temporary directory for this test run
var testRunDir string

func init() {
	syscall.Umask(0)

	var err error
	for i := 0; i < 10; i++ {
		// We must use a ramfs or else we get IO lag spikes of > 1 second
		testRunDir, err = ioutil.TempDir("/dev/shm", "quantumfsTest")
		if err != nil {
			continue
		}
		if err := os.Chmod(testRunDir, 777); err != nil {
			continue
		}
		return
	}
	panic(fmt.Sprintf("Unable to create temporary test directory: %v", err))
}

// Produce a request specific ctx variable to use for quantumfs internal calls
func (th *testHelper) newCtx() *ctx {
	reqId := atomic.AddUint64(&requestId, 1)
	c := th.qfs.c.dummyReq(reqId)
	c.Ctx.Vlog(qlog.LogTest, "Allocating request %d to test %s", reqId,
		th.TestName)
	return c
}

func (th *testHelper) remountFilesystem() {
	th.Log("Remounting filesystem")
	err := syscall.Mount("", th.TempDir+"/mnt", "", syscall.MS_REMOUNT, "")
	th.Assert(err == nil, "Unable to force vfs to drop dentry cache: %v", err)
}

// Modify the QuantumFS cache time to 100 milliseconds
func cacheTimeout100Ms(test *testHelper, config *QuantumFsConfig) {
	config.CacheTimeSeconds = 0
	config.CacheTimeNsecs = 100000
}

// Modify the QuantumFS flush delay to 100 milliseconds
func dirtyDelay100Ms(test *testHelper, config *QuantumFsConfig) {
	config.DirtyFlushDelay = 100 * time.Millisecond
}

// Extract namespace and workspace path from the absolute path of
// a workspaceroot
func (th *testHelper) getWorkspaceComponents(abspath string) (string,
	string, string) {

	relpath := th.RelPath(abspath)
	components := strings.Split(relpath, "/")

	return components[0], components[1], components[2]
}

// Convert an absolute workspace path to the matching WorkspaceRoot object
func (th *testHelper) getWorkspaceRoot(workspace string) *WorkspaceRoot {
	parts := strings.Split(th.RelPath(workspace), "/")
	wsr, ok := th.qfs.getWorkspaceRoot(&th.qfs.c,
		parts[0], parts[1], parts[2])
	th.Assert(ok, "WorkspaceRoot object for %s not found", workspace)

	return wsr
}

func (th *testHelper) getAccessList(workspace string) map[string]bool {
	return th.getWorkspaceRoot(workspace).getList()
}

func (th *testHelper) assertAccessList(testlist map[string]bool,
	wsrlist map[string]bool, message string) {

	eq := reflect.DeepEqual(testlist, wsrlist)
	msg := fmt.Sprintf("\ntestlist:%v\n, wsrlist:%v\n", testlist, wsrlist)
	message = message + msg
	th.Assert(eq, message)
}

func (th *testHelper) checkSparse(fileA string, fileB string, offset int,
	len int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	fdB, err := os.OpenFile(fileB, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileB for RDONLY")
	defer fdB.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")
	statB, err := fdB.Stat()
	th.Assert(err == nil, "Unable to fetch fileB stats")
	th.Assert(statB.Size() == statA.Size(), "file sizes don't match")

	rtnA := make([]byte, len)
	rtnB := make([]byte, len)

	for idx := int64(0); idx+int64(len) < statA.Size(); idx += int64(offset) {
		var readA int
		for readA < len {
			readIt, err := fdA.ReadAt(rtnA[readA:], idx+int64(readA))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileA at %d", idx)
			readA += readIt
		}

		var readB int
		for readB < len {
			readIt, err := fdB.ReadAt(rtnB[readB:], idx+int64(readB))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileB at %d", idx)
			readB += readIt
		}
		th.Assert(bytes.Equal(rtnA, rtnB), "data mismatch, %v vs %v",
			rtnA, rtnB)
	}
}

func (th *testHelper) checkZeroSparse(fileA string, offset int) {
	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")

	rtnA := make([]byte, 1)
	for idx := int64(0); idx < statA.Size(); idx += int64(offset) {
		_, err := fdA.ReadAt(rtnA, idx)

		if err == io.EOF {
			return
		}
		th.Assert(err == nil,
			"Error while reading from fileA at %d", idx)

		th.Assert(bytes.Equal(rtnA, []byte{0}), "file %s not zeroed",
			fileA)
	}
}
