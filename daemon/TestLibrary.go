// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library

import "bytes"
import "fmt"
import "io/ioutil"
import "math/rand"
import "os"
import "reflect"
import "runtime"
import "runtime/debug"
import "sort"
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

import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

const fusectlPath = "/sys/fs/fuse/"

type QuantumFsTest func(test *TestHelper)

type LogscanError struct {
	logFile           string
	shouldFailLogscan bool
	testName          string
}

var ErrorMutex sync.Mutex
var ErrorLogs []LogscanError

var timeMutex sync.Mutex
var timeBuckets []testutils.TimeData

//NoStdOut prints nothing to stdout
func NoStdOut(format string, args ...interface{}) error {
	return nil
}

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test QuantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, true, nil)
}

// If you need to initialize the QuantumFS instance in some special way,
// then use this variant.
func runTestNoQfs(t *testing.T, test QuantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, false, nil)
}

// configModifier is a function which is given the default configuration and should
// make whichever modifications the test requires in place.
type configModifierFunc func(test *TestHelper, config *QuantumFsConfig)

// If you need to initialize QuantumFS with a special configuration, but not poke
// into its internals before the test proper begins, use this.
func runTestCustomConfig(t *testing.T, configModifier configModifierFunc,
	test QuantumFsTest) {

	t.Parallel()
	runTestCommon(t, test, true, configModifier)
}

// If you need to initialize the QuantumFS instance in some special way and the test
// is relatively expensive, then use this variant.
func runTestNoQfsExpensiveTest(t *testing.T, test QuantumFsTest) {
	runTestCommon(t, test, false, nil)
}

// If you have a test which is expensive in terms of CPU time, then use
// runExpensiveTest() which will not run it at the same time as other tests. This is
// to prevent multiple expensive tests from running concurrently and causing each
// other to time out due to CPU starvation.
func runExpensiveTest(t *testing.T, test QuantumFsTest) {
	runTestCommon(t, test, true, nil)
}

func runTestCommon(t *testing.T, test QuantumFsTest, startDefaultQfs bool,
	configModifier configModifierFunc) {

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
	testResultChan := make(chan string, 2)
	startTime := time.Now()
	cachePath := TestRunDir + "/" + testName
	logger := qlog.NewQlogExt(cachePath+"/ramfs", 60*10000*24, NoStdOut)

	th := &TestHelper{}
	th.BaseInit(t, testName, testResultChan, startTime, cachePath, logger)
	th.TempDir = TestRunDir + "/" + th.TestName
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

	th.Log("Finished test preamble, starting test proper")
	beforeTest := time.Now()
	go th.Execute(test)

	var testResult string

	select {
	case <-time.After(1500 * time.Millisecond):
		testResult = "ERROR: TIMED OUT"

	case testResult = <-th.TestResult:
	}

	// Record how long the test took so we can make a histogram
	afterTest := time.Now()
	timeMutex.Lock()
	timeBuckets = append(timeBuckets,
		testutils.TimeData{
			Duration: afterTest.Sub(beforeTest),
			TestName: testName,
		})
	timeMutex.Unlock()

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
	}
}

// Execute the quantumfs test.
func (th *TestHelper) Execute(test QuantumFsTest) {
	// Catch any panics and covert them into test failures
	defer func(th *TestHelper) {
		err := recover()
		trace := ""

		// If the test passed pass that fact back to runTest()
		if err == nil {
			err = ""
		} else {
			// Capture the stack trace of the failure
			trace = utils.BytesToString(debug.Stack())
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

		// This can hang if the channel isn't buffered because in some rare
		// situations the other side isn't there to read from the channel
		th.TestResult <- result
	}(th)

	test(th)
}

func abortFuse(th *TestHelper) {
	if th.fuseConnection == 0 {
		// Nothing to abort
		return
	}

	// Forcefully abort the filesystem so it can be unmounted
	th.T.Logf("Aborting FUSE connection %d", th.fuseConnection)
	path := fmt.Sprintf("%s/connections/%d/abort", fusectlPath,
		th.fuseConnection)
	abort, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		// We cannot abort so we won't terminate. We are
		// truly wedged.
		th.Log("ERROR: Failed to abort FUSE connection (open)")
	}

	if _, err := abort.Write([]byte("1")); err != nil {
		th.Log("ERROR: Failed to abort FUSE connection (write)")
	}

	abort.Close()
}

// EndTest cleans up the testing environment after the test has finished
func (th *TestHelper) EndTest() {
	exception := recover()

	if th.api != nil {
		th.api.Close()
	}

	if th.qfs != nil && th.qfs.server != nil {
		if exception != nil {
			th.T.Logf("Failed with exception, forcefully unmounting: %v",
				exception)
			th.Log("Failed with exception, forcefully unmounting: %v",
				exception)
			abortFuse(th)
		}

		if err := th.qfs.server.Unmount(); err != nil {
			abortFuse(th)

			runtime.GC()

			if err := th.qfs.server.Unmount(); err != nil {
				th.Log("ERROR: Failed to unmount quantumfs "+
					"instance after aborting: %s", err.Error())
			}
			th.Log("ERROR: Failed to unmount quantumfs instance, "+
				"are you leaking a file descriptor?: %s",
				err.Error())
		}
	}

	if th.TempDir != "" {
		th.waitToBeUnmounted()
		th.waitForQuantumFsToFinish()
		time.Sleep(1 * time.Second)

		if testFailed := th.logscan(); !testFailed {
			if err := os.RemoveAll(th.TempDir); err != nil {
				th.T.Fatalf("Failed to cleanup temporary mount "+
					"point: %v", err)
			}
		}
	} else {
		th.T.Fatalf("No temporary directory available for logs")
	}

	if exception != nil {
		th.T.Fatalf("Test failed with exception: %v", exception)
	}
}

func (th *TestHelper) waitToBeUnmounted() {
	for i := 0; i < 100; i++ {
		mounts, err := ioutil.ReadFile("/proc/self/mountinfo")
		if err == nil {
			mounts := utils.BytesToString(mounts)
			if !strings.Contains(mounts, th.TempDir) {
				th.Log("Waited %d times to unmount", i)
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	th.Log("ERROR: Filesystem didn't unmount in time")
}

// Check the test output for errors
func (th *TestHelper) logscan() (foundErrors bool) {
	// Check the format string map for the log first to speed this up
	logFile := th.TempDir + "/ramfs/qlog"
	errorsPresent := qlog.LogscanSkim(logFile)

	// Nothing went wrong if either we should fail and there were errors,
	// or we shouldn't fail and there weren't errors
	if th.shouldFailLogscan == errorsPresent {
		return false
	}

	// There was a problem
	ErrorMutex.Lock()
	ErrorLogs = append(ErrorLogs, LogscanError{
		logFile:           logFile,
		shouldFailLogscan: th.shouldFailLogscan,
		testName:          th.TestName,
	})
	ErrorMutex.Unlock()

	if !th.shouldFailLogscan {
		th.T.Fatalf("Test FAILED due to FATAL messages\n")
	} else {
		th.T.Fatalf("Test FAILED due to missing FATAL messages\n")
	}

	return true
}

func OutputLogError(errInfo LogscanError) (summary string) {

	errors := make([]string, 0, 10)
	testOutputRaw := qlog.ParseLogsRaw(errInfo.logFile)
	sort.Sort(qlog.SortByTimePtr(testOutputRaw))

	var buffer bytes.Buffer

	extraLines := 0
	for _, rawLine := range testOutputRaw {
		line := rawLine.ToString()
		buffer.WriteString(line)

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
			"--- Test %s FAILED\n\n\n", errInfo.testName,
			buffer.String(), errInfo.testName)
		return fmt.Sprintf("--- Test %s FAILED due to errors:\n%s\n",
			errInfo.testName, strings.Join(errors, "\n"))
	} else {
		fmt.Printf("Test %s FAILED due to missing FATAL messages."+
			" Dumping Logs:\n%s\n--- Test %s FAILED\n\n\n",
			errInfo.testName, buffer.String(), errInfo.testName)
		return fmt.Sprintf("--- Test %s FAILED\nExpected errors, but found"+
			" none.\n", errInfo.testName)
	}
}

// TestHelper holds the variables important to maintain the state of testing in a
// package. This helper is more of a namespacing mechanism than a coherent object.
type TestHelper struct {
	mutex             sync.Mutex // Protects a mishmash of the members
	qfs               *QuantumFs
	qfsWait           sync.WaitGroup
	fuseConnection    int
	api               *quantumfs.Api
	shouldFailLogscan bool
	testutils.TestHelper
}

func (th *TestHelper) defaultConfig() QuantumFsConfig {
	mountPath := th.TempDir + "/mnt"

	config := QuantumFsConfig{
		CachePath:        th.TempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  30 * time.Second,
		MemLogBytes:      uint64(qlog.DefaultMmapSize),
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(""),
		DurableStore:     processlocal.NewDataStore(""),
	}
	return config
}

// StartDefaultQuantumFs start the qfs daemon with default config.
func (th *TestHelper) StartDefaultQuantumFs() {
	config := th.defaultConfig()
	th.startQuantumFs(config)
}

// If the filesystem panics, abort it and unmount it to prevent the test binary from
// hanging.
func serveSafely(th *TestHelper) {
	defer func(th *TestHelper) {
		exception := recover()
		if exception != nil {
			if th.fuseConnection != 0 {
				abortFuse(th)
			}
			th.T.Fatalf("FUSE panic'd: %v", exception)
		}
	}(th)

	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "QuantumFS",
		Name:          th.TestName,
		Options:       make([]string, 0),
	}
	mountOptions.Options = append(mountOptions.Options, "suid")
	mountOptions.Options = append(mountOptions.Options, "dev")

	th.qfsWait.Add(1)
	defer th.qfsWait.Done()
	th.qfs.Serve(mountOptions)
}

func (th *TestHelper) startQuantumFs(config QuantumFsConfig) {
	if err := os.MkdirAll(config.CachePath, 0777); err != nil {
		th.T.Fatalf("Unable to setup test ramfs path")
	}

	th.Log("Instantiating quantumfs instance...")
	quantumfs := NewQuantumFsLogs(config, th.Logger)
	th.qfs = quantumfs

	th.Log("Waiting for QuantumFs instance to start...")

	go serveSafely(th)

	th.fuseConnection = findFuseConnection(th.testCtx(), config.MountPath)
	th.Assert(th.fuseConnection != -1, "Failed to find mount")
	th.Log("QuantumFs instance started")
}

func (th *TestHelper) waitForQuantumFsToFinish() {
	th.qfsWait.Wait()
}

func (th *TestHelper) getApi() *quantumfs.Api {
	if th.api != nil {
		return th.api
	}

	th.api = quantumfs.NewApiWithPath(th.absPath(quantumfs.ApiPath))
	return th.api
}

// Make the given path absolute to the mount root
func (th *TestHelper) absPath(path string) string {
	return th.TempDir + "/mnt/" + path
}

// Make the given path relative to the mount root
func (th *TestHelper) relPath(path string) string {
	return strings.TrimPrefix(path, th.TempDir+"/mnt/")
}

// Extract namespace and workspace path from the absolute path of
// a workspaceroot
func (th *TestHelper) getWorkspaceComponents(abspath string) (string,
	string, string) {

	relpath := th.relPath(abspath)
	components := strings.Split(relpath, "/")

	return components[0], components[1], components[2]
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

func (th *TestHelper) nullWorkspaceRel() string {
	type_ := quantumfs.NullTypespaceName
	name_ := quantumfs.NullNamespaceName
	work_ := quantumfs.NullWorkspaceName
	return type_ + "/" + name_ + "/" + work_
}

func (th *TestHelper) nullWorkspace() string {
	return th.absPath(th.nullWorkspaceRel())
}

// Create a new workspace to test within
// Returns the absolute path of the workspace
func (th *TestHelper) NewWorkspace() string {
	return th.newWorkspace()
}

func (th *TestHelper) newWorkspace() string {
	api := th.getApi()

	type_ := randomNamespaceName(8)
	name_ := randomNamespaceName(10)
	work_ := randomNamespaceName(8)

	src := th.nullWorkspaceRel()
	dst := type_ + "/" + name_ + "/" + work_

	err := api.Branch(src, dst)
	th.Assert(err == nil, "Failed to branch workspace: %v", err)

	return th.absPath(dst)
}

// Branch existing workspace into new random name
//
// Returns the relative path of the new workspace.
func (th *TestHelper) branchWorkspace(original string) string {
	src := th.relPath(original)
	dst := randomNamespaceName(8) + "/" + randomNamespaceName(10) +
		"/" + randomNamespaceName(8)

	api := th.getApi()
	err := api.Branch(src, dst)

	th.Assert(err == nil, "Failed to branch workspace: %s -> %s: %v", src, dst,
		err)

	return dst
}

// Sync all the active workspaces
func (th *TestHelper) syncAllWorkspaces() {
	api := th.getApi()
	err := api.SyncAll()

	th.Assert(err == nil, "Error when syncing all workspaces: %v", err)
}

// Retrieve a list of FileDescriptor from an Inode
func (th *TestHelper) fileDescriptorFromInodeNum(inodeNum uint64) []*FileDescriptor {
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
func (th *TestHelper) getInodeNum(path string) InodeId {
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	th.Assert(err == nil, "Error grabbing file inode (%s): %v", path, err)

	return InodeId(stat.Ino)
}

// Retrieve the Inode from Quantumfs. Returns nil is not instantiated
func (th *TestHelper) getInode(path string) Inode {
	inodeNum := th.getInodeNum(path)
	return th.qfs.inodeNoInstantiate(&th.qfs.c, inodeNum)
}

// Retrieve the rootId of the given workspace
func (th *TestHelper) workspaceRootId(typespace string, namespace string,
	workspace string) quantumfs.ObjectKey {

	key, err := th.qfs.c.workspaceDB.Workspace(&th.newCtx().Ctx,
		typespace, namespace, workspace)
	th.Assert(err == nil, "Error fetching key")

	return key
}

// Global test request ID incremented for all the running tests
var requestId = uint64(1000000000)

// Temporary directory for this test run
var TestRunDir string

func init() {
	fmt.Printf("1. daemon testutils.TestRunDir %s\n", TestRunDir)
	fmt.Printf("1. daemon testutils.TestRunDir %s\n", testutils.TestRunDir)

	timeMutex = testutils.TimeMutex
	timeBuckets = testutils.TimeBuckets
	TestRunDir = testutils.TestRunDir

	fmt.Printf("2. daemon testutils.TestRunDir %s\n", testutils.TestRunDir)
}

// Produce a request specific ctx variable to use for quantumfs internal calls
func (th *TestHelper) newCtx() *ctx {
	reqId := atomic.AddUint64(&requestId, 1)
	c := th.qfs.c.dummyReq(reqId)
	c.Ctx.Vlog(qlog.LogTest, "Allocating request %d to test %s", reqId,
		th.TestName)
	return c
}

// Produce a test infrastructure ctx variable for use with QuantumFS utility
// functions.
func (th *TestHelper) testCtx() *ctx {
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

// Init initializes the private memnbers of TestHelper
func (th *TestHelper) Init(t *testing.T, testName string, testResult chan string,
	startTime time.Time, cachePath string, logger *qlog.Qlog) {

	//th.t = t
	//th.TestName = testName
	//th.TestResult = testResult
	//th.startTime = startTime
	//th.cachePath = cachePath
	//th.tempDir = cachePath
	//th.logger = logger
	th.BaseInit(t, testName, testResult, startTime, cachePath, logger)
}

type crashOnWrite struct {
	FileHandle
}

func (crash *crashOnWrite) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	panic("Intentional crash")
}

// Convert an absolute workspace path to the matching WorkspaceRoot object
func (th *TestHelper) getWorkspaceRoot(workspace string) *WorkspaceRoot {
	parts := strings.Split(th.relPath(workspace), "/")
	wsr, ok := th.qfs.getWorkspaceRoot(&th.qfs.c,
		parts[0], parts[1], parts[2])

	th.Assert(ok, "WorkspaceRoot object for %s not found", workspace)

	return wsr
}

func (th *TestHelper) getAccessList(workspace string) map[string]bool {
	return th.getWorkspaceRoot(workspace).getList()
}

func (th *TestHelper) AssertAccessList(testlist map[string]bool,
	wsrlist map[string]bool, message string) {

	eq := reflect.DeepEqual(testlist, wsrlist)
	msg := fmt.Sprintf("\ntestlist:%v\n, wsrlist:%v\n", testlist, wsrlist)
	message = message + msg
	th.Assert(eq, message)
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

// Change the UID/GID the test thread to the given values. Use -1 not to change
// either the UID or GID.
func (th *TestHelper) setUidGid(uid int, gid int) {
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
		th.Assert(err == nil, "Faild to change test EGID: %v", err)
	}

	if uid != -1 {
		err := syscall.Setreuid(-1, uid)
		if err != nil {
			syscall.Setregid(-1, 0)
			runtime.UnlockOSThread()
		}
		th.Assert(err == nil, "Failed to change test EUID: %v", err)
	}

}

// Set the UID and GID back to the defaults
func (th *TestHelper) setUidGidToDefault() {
	defer runtime.UnlockOSThread()

	// Test always runs as root, so its euid and egid is 0
	err1 := syscall.Setreuid(-1, 0)
	err2 := syscall.Setregid(-1, 0)

	th.Assert(err1 == nil, "Failed to set test EGID back to 0: %v", err1)
	th.Assert(err2 == nil, "Failed to set test EUID back to 0: %v", err2)
}

// A lot of times you're trying to do a test and you get error codes. The errors
// often describe the problem better than any th.Assert message, so use them
func (th *TestHelper) AssertNoErr(err error) {
	if err != nil {
		th.Assert(false, err.Error())
	}
}

func (th *TestHelper) remountFilesystem() {
	th.Log("Remounting filesystem")
	err := syscall.Mount("", th.TempDir+"/mnt", "", syscall.MS_REMOUNT, "")
	th.Assert(err == nil, "Unable to force vfs to drop dentry cache: %v", err)
}

// Modify the QuantumFS cache time to 100 milliseconds
func cacheTimeout100Ms(test *TestHelper, config *QuantumFsConfig) {
	config.CacheTimeSeconds = 0
	config.CacheTimeNsecs = 100000
}

// Modify the QuantumFS flush delay to 100 milliseconds
func dirtyDelay100Ms(test *TestHelper, config *QuantumFsConfig) {
	config.DirtyFlushDelay = 100 * time.Millisecond
}
