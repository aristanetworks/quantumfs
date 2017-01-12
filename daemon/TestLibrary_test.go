// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library

import "bytes"
import "errors"
import "flag"
import "fmt"
import "io"
import "io/ioutil"
import "math/rand"
import "os"
import "reflect"
import "runtime"
import "runtime/debug"
import "strings"
import "strconv"
import "sync"
import "sync/atomic"
import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/qlog"

import "github.com/hanwen/go-fuse/fuse"

const fusectlPath = "/sys/fs/fuse/"

type quantumFsTest func(test *testHelper)

type logscanError struct {
	logFile           string
	shouldFailLogscan bool
	testName          string
}

var errorMutex sync.Mutex
var errorLogs []logscanError

func noStdOut(format string, args ...interface{}) error {
	// Do nothing
	return nil
}

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true)
}

// If you need to initialize the QuantumFS instance in some special way, then use
// this variant.
func runTestNoQfs(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, false)
}

// If you need to initialize the QuantumFS instance in some special way and the test
// is relatively expensive, then use this variant.
func runTestNoQfsExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, false)
}

// If you have a test which is expensive in terms of CPU time, then use
// runExpensiveTest() which will not run it at the same time as other tests. This is
// to prevent multiple expensive tests from running concurrently and causing each
// other to time out due to CPU starvation.
func runExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, true)
}

func runTestCommon(t *testing.T, test quantumFsTest, startDefaultQfs bool) {
	// Since we grab the test name from the backtrace, it must always be an
	// identical number of frames back to the name of the test. Otherwise
	// multiple tests will end up using the same temporary directory and nothing
	// will work.
	//
	// 2 <testname>
	// 1 runTest/runExpensiveTest
	// 0 runTestCommon
	testPc, _, _, _ := runtime.Caller(2)
	testName := runtime.FuncForPC(testPc).Name()
	lastSlash := strings.LastIndex(testName, "/")
	testName = testName[lastSlash+1:]
	cachePath := testRunDir + "/" + testName
	th := &testHelper{
		t:          t,
		testName:   testName,
		testResult: make(chan string),
		startTime:  time.Now(),
		cachePath:  cachePath,
		logger: qlog.NewQlogExt(cachePath+"/ramfs", 60*10000*24,
			noStdOut),
	}
	th.createTestDirs()

	defer th.endTest()

	// Allow tests to run for up to 1 seconds before considering them timed out.
	// If we are going to start a standard QuantumFS instance we can start the
	// timer before the test proper and therefore avoid false positive test
	// failures due to timeouts caused by system slowness as we try to mount
	// dozens of FUSE filesystems at once.
	if startDefaultQfs {
		th.startDefaultQuantumFs()
	}

	th.log("Finished test preamble, starting test proper")
	go th.execute(test)

	var testResult string

	select {
	case <-time.After(1000 * time.Millisecond):
		testResult = "ERROR: TIMED OUT"

	case testResult = <-th.testResult:
	}

	if !th.shouldFail && testResult != "" {
		th.log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.shouldFail && testResult == "" {
		th.log("ERROR: Test is expected to fail, but didn't")
	}
}

func (th *testHelper) execute(test quantumFsTest) {
	// Catch any panics and covert them into test failures
	defer func(th *testHelper) {
		err := recover()
		trace := ""

		// If the test passed pass that fact back to runTest()
		if err == nil {
			err = ""
		} else {
			// Capture the stack trace of the failure
			trace = BytesToString(debug.Stack())
			trace = strings.SplitN(trace, "\n", 8)[7]
		}

		var result string
		switch err.(type) {
		default:
			result = fmt.Sprintf("Unknown panic type: %v", err)
		case string:
			result = err.(string)
		case error:
			result = err.(error).Error()
		}

		if trace != "" {
			result += "\nStack Trace:\n" + trace
		}

		th.testResult <- result
	}(th)

	test(th)
}

func abortFuse(th *testHelper) {
	if th.fuseConnection == 0 {
		// Nothing to abort
		return
	}

	// Forcefully abort the filesystem so it can be unmounted
	th.t.Logf("Aborting FUSE connection %d", th.fuseConnection)
	path := fmt.Sprintf("%s/connections/%d/abort", fusectlPath,
		th.fuseConnection)
	abort, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		// We cannot abort so we won't terminate. We are
		// truly wedged.
		th.log("ERROR: Failed to abort FUSE connection (open)")
	}

	if _, err := abort.Write([]byte("1")); err != nil {
		th.log("ERROR: Failed to abort FUSE connection (write)")
	}

	abort.Close()
}

// endTest cleans up the testing environment after the test has finished
func (th *testHelper) endTest() {
	exception := recover()

	if th.api != nil {
		th.api.Close()
	}

	if th.qfs != nil && th.qfs.server != nil {
		if exception != nil {
			th.t.Logf("Failed with exception, forcefully unmounting: %v",
				exception)
			th.log("Failed with exception, forcefully unmounting: %v",
				exception)
			abortFuse(th)
		}

		if err := th.qfs.server.Unmount(); err != nil {
			abortFuse(th)

			runtime.GC()

			if err := th.qfs.server.Unmount(); err != nil {
				th.log("ERROR: Failed to unmount quantumfs "+
					"instance after aborting: %v", err)
			}
			th.log("ERROR: Failed to unmount quantumfs instance, "+
				"are you leaking a file descriptor?: %v", err)
		}
	}

	if th.tempDir != "" {
		th.waitToBeUnmounted()
		time.Sleep(1 * time.Second)

		if testFailed := th.logscan(); !testFailed {
			if err := os.RemoveAll(th.tempDir); err != nil {
				th.t.Fatalf("Failed to cleanup temporary mount "+
					"point: %v", err)
			}
		}
	} else {
		th.t.Fatalf("No temporary directory available for logs")
	}

	if exception != nil {
		th.t.Fatalf("Test failed with exception: %v", exception)
	}
}

func (th *testHelper) waitToBeUnmounted() {
	for i := 0; i < 100; i++ {
		mounts, err := ioutil.ReadFile("/proc/self/mountinfo")
		if err == nil {
			mounts := BytesToString(mounts)
			if !strings.Contains(mounts, th.tempDir) {
				th.log("Waited %d times to unmount", i)
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	th.log("ERROR: Filesystem didn't unmount in time")
}

// Repeatedly check the condition by calling the function until that function returns
// true.
//
// No timeout is provided beyond the normal test timeout.
func (th *testHelper) waitFor(description string, condition func() bool) {
	th.log("Started waiting for %s", description)
	for {
		if condition() {
			th.log("Finished waiting for %s", description)
			return
		} else {
			th.log("Condition not satisfied")
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// Check the test output for errors
func (th *testHelper) logscan() (foundErrors bool) {
	// Check the format string map for the log first to speed this up
	logFile := th.tempDir + "/ramfs/qlog"
	errorsPresent := qlog.LogscanSkim(logFile)

	// Nothing went wrong if either we should fail and there were errors,
	// or we shouldn't fail and there weren't errors
	if th.shouldFailLogscan == errorsPresent {
		return false
	}

	// There was a problem
	errorMutex.Lock()
	errorLogs = append(errorLogs, logscanError{
		logFile:           logFile,
		shouldFailLogscan: th.shouldFailLogscan,
		testName:          th.testName,
	})
	errorMutex.Unlock()

	if !th.shouldFailLogscan {
		th.t.Fatalf("Test FAILED due to FATAL messages\n")
	} else {
		th.t.Fatalf("Test FAILED due to missing FATAL messages\n")
	}

	return true
}

func outputLogError(errInfo logscanError) (summary string) {
	errors := make([]string, 0, 10)
	testOutput := qlog.ParseLogs(errInfo.logFile)

	lines := strings.Split(testOutput, "\n")

	extraLines := 0
	for _, line := range lines {
		if strings.Contains(line, "PANIC") ||
			strings.Contains(line, "WARN") ||
			strings.Contains(line, "ERROR") {
			extraLines = 2
		}

		// Output a couple extra lines after an ERROR
		if extraLines > 0 {
			// ensure a single line isn't ridiculously long
			if len(line) > 255 {
				line = line[:255] + "...TRUNCATED"
			}

			errors = append(errors, line)
			extraLines--
		}
	}

	if !errInfo.shouldFailLogscan {
		fmt.Printf("Test %s FAILED due to ERROR. Dumping Logs:\n%s\n"+
			"--- Test %s FAILED\n\n\n", errInfo.testName, testOutput,
			errInfo.testName)
		return fmt.Sprintf("--- Test %s FAILED due to errors:\n%s\n",
			errInfo.testName, strings.Join(errors, "\n"))
	} else {
		fmt.Printf("Test %s FAILED due to missing FATAL messages."+
			" Dumping Logs:\n%s\n--- Test %s FAILED\n\n\n",
			errInfo.testName, testOutput, errInfo.testName)
		return fmt.Sprintf("--- Test %s FAILED\nExpected errors, but found"+
			" none.\n", errInfo.testName)
	}
}

// This helper is more of a namespacing mechanism than a coherent object
type testHelper struct {
	mutex             sync.Mutex // Protects a mishmash of the members
	t                 *testing.T
	testName          string
	qfs               *QuantumFs
	cachePath         string
	logger            *qlog.Qlog
	tempDir           string
	fuseConnection    int
	api               *quantumfs.Api
	testResult        chan string
	startTime         time.Time
	shouldFail        bool
	shouldFailLogscan bool
}

func (th *testHelper) createTestDirs() {
	th.tempDir = testRunDir + "/" + th.testName

	mountPath := th.tempDir + "/mnt"
	os.MkdirAll(mountPath, 0777)
	th.log("Using mountpath %s", mountPath)

	os.MkdirAll(th.tempDir+"/ether", 0777)
}

func (th *testHelper) defaultConfig() QuantumFsConfig {
	mountPath := th.tempDir + "/mnt"

	config := QuantumFsConfig{
		CachePath:        th.tempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		MemLogBytes:      uint64(qlog.DefaultMmapSize),
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(""),
		DurableStore:     processlocal.NewDataStore(""),
	}
	return config
}

func (th *testHelper) startDefaultQuantumFs() {
	config := th.defaultConfig()

	th.startQuantumFs(config)
}

// If the filesystem panics, abort it and unmount it to prevent the test binary from
// hanging.
func serveSafely(th *testHelper) {
	defer func(th *testHelper) {
		exception := recover()
		if exception != nil {
			if th.fuseConnection != 0 {
				abortFuse(th)
			}
			th.t.Fatalf("FUSE panic'd: %v", exception)
		}
	}(th)

	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "cluster",
		Name:          th.testName,
		Options:       make([]string, 0),
	}
	mountOptions.Options = append(mountOptions.Options, "suid")
	mountOptions.Options = append(mountOptions.Options, "dev")

	th.qfs.Serve(mountOptions)
}

func (th *testHelper) startQuantumFs(config QuantumFsConfig) {
	if err := os.MkdirAll(config.CachePath, 0777); err != nil {
		th.t.Fatalf("Unable to setup test ramfs path")
	}

	th.log("Instantiating quantumfs instance...")
	quantumfs := NewQuantumFsLogs(config, th.logger)
	th.qfs = quantumfs

	th.log("Waiting for QuantumFs instance to start...")

	go serveSafely(th)

	th.fuseConnection = findFuseConnection(th.testCtx(), config.MountPath)
	th.assert(th.fuseConnection != -1, "Failed to find mount")
	th.log("QuantumFs instance started")
}

func (th *testHelper) log(format string, args ...interface{}) error {
	th.logger.Log(qlog.LogTest, qlog.TestReqId, 1,
		"[%s] "+format, append([]interface{}{th.testName},
			args...)...)

	return nil
}

func (th *testHelper) getApi() *quantumfs.Api {
	if th.api != nil {
		return th.api
	}

	th.api = quantumfs.NewApiWithPath(th.absPath(quantumfs.ApiPath))
	return th.api
}

// Make the given path absolute to the mount root
func (th *testHelper) absPath(path string) string {
	return th.tempDir + "/mnt/" + path
}

// Make the given path relative to the mount root
func (th *testHelper) relPath(path string) string {
	return strings.TrimPrefix(path, th.tempDir+"/mnt/")
}

// Extract namespace and workspace path from the absolute path of
// a workspaceroot
func (th *testHelper) getWorkspaceComponents(abspath string) (string, string) {
	relpath := th.relPath(abspath)
	components := strings.Split(relpath, "/")

	return components[0], components[1]
}

// Return a random namespace/workspace name of given length
func randomNamespaceName(size int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz" + "0123456789-." +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	result := ""
	for i := 0; i < size; i++ {
		result += string(chars[rand.Intn(len(chars))])
	}

	return result
}

func TestRandomNamespaceName(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		name1 := randomNamespaceName(8)
		name2 := randomNamespaceName(8)
		name3 := randomNamespaceName(10)

		test.assert(len(name1) == 8, "name1 wrong length: %d", len(name1))
		test.assert(name1 != name2, "name1 == name2: '%s'", name1)
		test.assert(len(name3) == 10, "name3 wrong length: %d", len(name1))
	})
}

func (th *testHelper) nullWorkspaceRel() string {
	return quantumfs.NullNamespaceName + "/" + quantumfs.NullWorkspaceName
}

func (th *testHelper) nullWorkspace() string {
	return th.absPath(th.nullWorkspaceRel())
}

// Create a new workspace to test within
//
// Returns the absolute path of the workspace
func (th *testHelper) newWorkspace() string {
	api := th.getApi()

	src := th.nullWorkspaceRel()
	dst := randomNamespaceName(8) + "/" + randomNamespaceName(10)

	err := api.Branch(src, dst)
	th.assert(err == nil, "Failed to branch workspace: %v", err)

	return th.absPath(dst)
}

// Branch existing workspace into new random name
//
// Returns the relative path of the new workspace.
func (th *testHelper) branchWorkspace(original string) string {
	src := th.relPath(original)
	dst := randomNamespaceName(8) + "/" + randomNamespaceName(10)

	api := th.getApi()
	err := api.Branch(src, dst)

	th.assert(err == nil, "Failed to branch workspace: %s -> %s: %v", src, dst,
		err)

	return dst
}

// Sync all the active workspaces
func (th *testHelper) syncAllWorkspaces() {
	api := th.getApi()
	err := api.SyncAll()

	th.assert(err == nil, "Error when syncing all workspaces: %v", err)
}

// Retrieve a list of FileDescriptor from an Inode
func (th *testHelper) fileDescriptorFromInodeNum(inodeNum uint64) []*FileDescriptor {
	handles := make([]*FileDescriptor, 0)

	th.qfs.mapMutex.Lock()

	for _, file := range th.qfs.fileHandles {
		fh, ok := file.(*FileDescriptor)
		if !ok {
			continue
		}

		if fh.inodeNum == InodeId(inodeNum) {
			handles = append(handles, fh)
		}
	}

	th.qfs.mapMutex.Unlock()

	return handles
}

// Return the inode number from QuantumFS. Fails if the absolute path doesn't exist.
func (th *testHelper) getInodeNum(path string) InodeId {
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	th.assert(err == nil, "Error grabbing file inode: %v", err)

	return InodeId(stat.Ino)
}

// Retrieve the Inode from Quantumfs. Returns nil is not instantiated
func (th *testHelper) getInode(path string) Inode {
	inodeNum := th.getInodeNum(path)
	return th.qfs.inodeNoInstantiate(&th.qfs.c, inodeNum)
}

// Retrieve the rootId of the given workspace
func (th *testHelper) workspaceRootId(namespace string,
	workspace string) quantumfs.ObjectKey {

	key, err := th.qfs.c.workspaceDB.Workspace(&th.newCtx().Ctx,
		namespace, workspace)
	th.assert(err == nil, "Error fetching key")

	return key
}

// Global test request ID incremented for all the running tests
var requestId = uint64(1000000000)

// Temporary directory for this test run
var testRunDir string

func init() {
	syscall.Umask(0)

	var err error
	for i := 0; i < 10; i++ {
		testRunDir, err = ioutil.TempDir("", "quantumfsTest")
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

func TestMain(m *testing.M) {
	flag.Parse()

	if os.Getuid() != 0 {
		panic("quantumfs.daemon tests must be run as root")
	}

	// Disable Garbage Collection. Because the tests provide both the filesystem
	// and the code accessing that filesystem the program is reentrant in ways
	// opaque to the golang scheduler. Thus we can end up in a deadlock situation
	// between two threads:
	//
	// ThreadFS is the filesystem, ThreadT is the test
	//
	//   ThreadFS                    ThreadT
	//                               Start filesystem syscall
	//   Start executing response
	//   <GC Wait>                   <Queue GC wait after syscal return>
	//                        DEADLOCK
	//
	// Because the filesystem request is blocked waiting on GC and the syscall
	// will never return to allow GC to progress, the test program is deadlocked.
	origGC := debug.SetGCPercent(-1)

	// Precompute a bunch of our genData to save time during tests
	genData(40 * 1024 * 1024)

	// Setup an array for tests with errors to be logscanned later
	errorLogs = make([]logscanError, 0)

	result := m.Run()

	// We've finished running the tests and are about to do the full logscan.
	// This create a tremendous amount of garbage, so we must enable garbage
	// collection.
	runtime.GC()
	debug.SetGCPercent(origGC)

	testSummary := ""
	errorMutex.Lock()
	for i := 0; i < len(errorLogs); i++ {
		testSummary += outputLogError(errorLogs[i])
	}
	errorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	os.RemoveAll(testRunDir)
	os.Exit(result)
}

// Produce a request specific ctx variable to use for quantumfs internal calls
func (th *testHelper) newCtx() *ctx {
	reqId := atomic.AddUint64(&requestId, 1)
	c := th.qfs.c.dummyReq(reqId)
	c.Ctx.Vlog(qlog.LogTest, "Allocating request %d to test %s", reqId,
		th.testName)
	return c
}

// Produce a test infrastructure ctx variable for use with QuantumFS utility
// functions.
func (th *testHelper) testCtx() *ctx {
	return th.qfs.c.dummyReq(qlog.TestReqId)
}

//only to be used for some testing - not all functions will work with this
func (c *ctx) dummyReq(request uint64) *ctx {
	requestCtx := &ctx{
		Ctx: quantumfs.Ctx{
			Qlog:      c.Qlog,
			RequestId: request,
		},
		qfs:         c.qfs,
		config:      c.config,
		workspaceDB: c.workspaceDB,
		dataStore:   c.dataStore,
		fuseCtx:     nil,
	}
	return requestCtx
}

// assert the condition is true. If it is not true then fail the test with the given
// message
func (th *testHelper) assert(condition bool, format string, args ...interface{}) {
	if !condition {
		msg := fmt.Sprintf(format, args...)
		panic(msg)
	}
}

type TLA struct {
	mustContain bool   // Whether the log must or must not contain the text
	text        string // text which must or must not be in the log
	failMsg     string // Message to fail with
}

// Assert the test log contains the given text
func (th *testHelper) assertLogContains(text string, failMsg string) {
	th.assertTestLog([]TLA{TLA{true, text, failMsg}})
}

// Assert the test log doesn't contain the given text
func (th *testHelper) assertLogDoesNotContain(text string, failMsg string) {
	th.assertTestLog([]TLA{TLA{false, text, failMsg}})
}

func (th *testHelper) assertTestLog(logs []TLA) {
	logFile := th.tempDir + "/ramfs/qlog"
	logOutput := qlog.ParseLogs(logFile)

	for _, tla := range logs {
		exists := strings.Contains(logOutput, tla.text)
		th.assert(exists == tla.mustContain, tla.failMsg)
	}
}

type crashOnWrite struct {
	FileHandle
}

func (crash *crashOnWrite) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	panic("Intentional crash")
}

// If a quantumfs test fails then it may leave the filesystem mount hanging around in
// a blocked state. testHelper needs to forcefully abort and umount these to keep the
// system functional. Test this forceful unmounting here.
func TestPanicFilesystemAbort(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		test.shouldFailLogscan = true

		test.startDefaultQuantumFs()
		api := test.getApi()

		// Introduce a panicing error into quantumfs
		test.qfs.mapMutex.Lock()
		for k, v := range test.qfs.fileHandles {
			test.qfs.fileHandles[k] = &crashOnWrite{FileHandle: v}
		}
		test.qfs.mapMutex.Unlock()

		// panic Quantumfs
		api.Branch("_null/null", "test/crash")
	})
}

// If a test never returns from some event, such as an inifinite loop, the test
// should timeout and cleanup after itself.
func TestTimeout(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.shouldFail = true
		time.Sleep(60 * time.Second)

		// If we get here then the test library didn't time us out and we
		// sould fail this test.
		test.shouldFail = false
		test.assert(false, "Test didn't fail due to timeout")
	})
}

func printToFile(filename string, data string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0777)
	if file == nil || err != nil {
		return err
	}
	defer file.Close()

	written := 0
	for written < len(data) {
		var writeIt int
		writeIt, err = file.Write([]byte(data[written:]))
		written += writeIt
		if err != nil {
			return errors.New("Unable to write all data")
		}
	}

	return nil
}

func (test *testHelper) readTo(file *os.File, offset int, num int) []byte {
	rtn := make([]byte, num)

	for totalCount := 0; totalCount < num; {
		readIt, err := file.ReadAt(rtn[totalCount:],
			int64(offset+totalCount))
		if err == io.EOF {
			return rtn[:totalCount+readIt]
		}
		test.assert(err == nil, "Unable to read from file")
		totalCount += readIt
	}

	return rtn
}

func (test *testHelper) checkSparse(fileA string, fileB string, offset int,
	len int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	test.assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	fdB, err := os.OpenFile(fileB, os.O_RDONLY, 0777)
	test.assert(err == nil, "Unable to open fileB for RDONLY")
	defer fdB.Close()

	statA, err := fdA.Stat()
	test.assert(err == nil, "Unable to fetch fileA stats")
	statB, err := fdB.Stat()
	test.assert(err == nil, "Unable to fetch fileB stats")
	test.assert(statB.Size() == statA.Size(), "file sizes don't match")

	rtnA := make([]byte, len)
	rtnB := make([]byte, len)

	for idx := int64(0); idx+int64(len) < statA.Size(); idx += int64(offset) {
		var readA int
		for readA < len {
			readIt, err := fdA.ReadAt(rtnA[readA:], idx+int64(readA))

			if err == io.EOF {
				return
			}
			test.assert(err == nil,
				"Error while reading from fileA at %d", idx)
			readA += readIt
		}

		var readB int
		for readB < len {
			readIt, err := fdB.ReadAt(rtnB[readB:], idx+int64(readB))

			if err == io.EOF {
				return
			}
			test.assert(err == nil,
				"Error while reading from fileB at %d", idx)
			readB += readIt
		}
		test.assert(bytes.Equal(rtnA, rtnB), "data mismatch, %v vs %v",
			rtnA, rtnB)
	}
}

func (test *testHelper) checkZeroSparse(fileA string, offset int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	test.assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	statA, err := fdA.Stat()
	test.assert(err == nil, "Unable to fetch fileA stats")

	rtnA := make([]byte, 1)
	for idx := int64(0); idx < statA.Size(); idx += int64(offset) {
		_, err := fdA.ReadAt(rtnA, idx)

		if err == io.EOF {
			return
		}
		test.assert(err == nil,
			"Error while reading from fileA at %d", idx)

		test.assert(bytes.Equal(rtnA, []byte{0}), "file %s not zeroed",
			fileA)
	}
}

func (test *testHelper) fileSize(filename string) int64 {
	var stat syscall.Stat_t
	err := syscall.Stat(filename, &stat)
	test.assert(err == nil, "Error stat'ing test file: %v", err)
	return stat.Size
}

// Convert an absolute workspace path to the matching WorkspaceRoot object
func (test *testHelper) getWorkspaceRoot(workspace string) *WorkspaceRoot {
	parts := strings.Split(test.relPath(workspace), "/")
	wsr, ok := test.qfs.getWorkspaceRoot(&test.qfs.c, parts[0], parts[1])

	test.assert(ok, "WorkspaceRoot object for %s not found", workspace)

	return wsr
}

func (test *testHelper) getAccessList(workspace string) map[string]bool {
	return test.getWorkspaceRoot(workspace).getList()
}

func (test *testHelper) assertAccessList(testlist map[string]bool,
	wsrlist map[string]bool, message string) {

	eq := reflect.DeepEqual(testlist, wsrlist)
	msg := fmt.Sprintf("\ntestlist:%v\n, wsrlist:%v\n", testlist, wsrlist)
	message = message + msg
	test.assert(eq, message)
}

var genDataMutex sync.RWMutex
var precompGenData []byte
var genDataLast int

func genData(maxLen int) []byte {
	if maxLen > len(precompGenData) {
		// we need to expand the array
		genDataMutex.Lock()

		for len(precompGenData) <= maxLen {
			precompGenData = append(precompGenData,
				strconv.Itoa(genDataLast)...)
			genDataLast++
		}

		genDataMutex.Unlock()
	}
	genDataMutex.RLock()
	defer genDataMutex.RUnlock()

	return precompGenData[:maxLen]
}

func TestGenData(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		hardcoded := "012345678910111213141516171819202122232425262"
		data := genData(len(hardcoded))

		test.assert(bytes.Equal([]byte(hardcoded), data),
			"Data gen function off: %s vs %s", hardcoded, data)
	})
}

// Change the UID/GID the test thread to the given values. Use -1 not to change
// either the UID or GID.
func (test *testHelper) setUidGid(uid int, gid int) {
	// The quantumfs tests are run as root because some tests require
	// root privileges. However, root can read or write any file
	// irrespective of the file permissions. Obviously if we want to
	// test permissions then we cannot run as root.
	//
	// To accomplish this we lock this goroutine to a particular OS
	// thread, then we change the EUID of that thread to something which
	// isn't root. Finally at the end we need to restore the EUID of the
	// thread before unlocking ourselves from that thread. If we do not
	// follow this precise cleanup order other tests or goroutines may
	// run using the other UID incorrectly.
	runtime.LockOSThread()
	if gid != -1 {
		err := syscall.Setregid(-1, gid)
		if err != nil {
			runtime.UnlockOSThread()
		}
		test.assert(err == nil, "Faild to change test EGID: %v", err)
	}

	if uid != -1 {
		err := syscall.Setreuid(-1, uid)
		if err != nil {
			syscall.Setregid(-1, 0)
			runtime.UnlockOSThread()
		}
		test.assert(err == nil, "Failed to change test EUID: %v", err)
	}

}

// Set the UID and GID back to the defaults
func (test *testHelper) setUidGidToDefault() {
	defer runtime.UnlockOSThread()

	// Test always runs as root, so its euid and egid is 0
	err1 := syscall.Setreuid(-1, 0)
	err2 := syscall.Setregid(-1, 0)

	test.assert(err1 == nil, "Failed to set test EGID back to 0: %v", err1)
	test.assert(err2 == nil, "Failed to set test EUID back to 0: %v", err2)
}

// A lot of times you're trying to do a test and you get error codes. The errors
// often describe the problem better than any test.assert message, so use them
func (test *testHelper) assertNoErr(err error) {
	if err != nil {
		test.assert(false, err.Error())
	}
}
