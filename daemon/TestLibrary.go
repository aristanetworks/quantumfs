// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library for daemon package

import "fmt"
import "io/ioutil"
import "math/rand"
import "os"
import "runtime"
import "runtime/debug"
import "strings"
import "strconv"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

import "github.com/hanwen/go-fuse/fuse"

const fusectlPath = "/sys/fs/fuse/"

type QuantumFsTest func(test *TestHelper)

//NoStdOut prints nothing to stdout
func NoStdOut(format string, args ...interface{}) error {
	return nil
}

// TestHelper holds the variables important to maintain the state of testing
// in a package which intends to use a QFS instance. daemon.TestHelper will
// need to be embedded in that package's testHelper.
type TestHelper struct {
	testutils.TestHelper
	qfs            *QuantumFs
	qfsWait        sync.WaitGroup
	fuseConnection int
	api            *quantumfs.Api
}

// CreateTestDirs makes the required directories for the test.
// These directories are inside TestRunDir
func (th *TestHelper) CreateTestDirs() {
	th.TempDir = TestRunDir + "/" + th.TestName

	mountPath := th.TempDir + "/mnt"
	os.MkdirAll(mountPath, 0777)
	th.Log("Using mountpath %s", mountPath)

	os.MkdirAll(th.TempDir+"/ether", 0777)
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

		if testFailed := th.Logscan(); !testFailed {
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

func (th *TestHelper) RestartQuantumFs() error {

	config := th.qfs.config
	th.api.Close()
	err := th.qfs.server.Unmount()
	if err != nil {
		return err
	}
	th.waitForQuantumFsToFinish()
	th.startQuantumFs(config)
	return nil
}

func (th *TestHelper) getApi() *quantumfs.Api {
	if th.api != nil {
		return th.api
	}

	api, err := quantumfs.NewApiWithPath(th.absPath(quantumfs.ApiPath))
	th.Assert(err == nil, "Error getting api: %v", err)
	th.api = api
	return th.api
}

// Make the given path absolute to the mount root
func (th *TestHelper) absPath(path string) string {
	return th.TempDir + "/mnt/" + path
}

// Make the given path relative to the mount root
func (th *TestHelper) RelPath(path string) string {
	return strings.TrimPrefix(path, th.TempDir+"/mnt/")
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
	type_ := quantumfs.NullSpaceName
	name_ := quantumfs.NullSpaceName
	work_ := quantumfs.NullSpaceName
	return type_ + "/" + name_ + "/" + work_
}

func (th *TestHelper) nullWorkspace() string {
	return th.absPath(th.nullWorkspaceRel())
}

func (th *TestHelper) newWorkspaceWithoutWritePerm() string {
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

func (th *TestHelper) NewWorkspace() string {
	return th.newWorkspace()
}

// Create a new workspace to test within
//
// Returns the absolute path of the workspace
func (th *TestHelper) newWorkspace() string {
	path := th.newWorkspaceWithoutWritePerm()

	api := th.getApi()
	err := api.EnableRootWrite(th.RelPath(path))
	th.Assert(err == nil, "Failed to enable write permission in workspace: %v",
		err)

	return path
}

func (th *TestHelper) branchWorkspaceWithoutWritePerm(original string) string {
	src := th.RelPath(original)
	dst := randomNamespaceName(8) + "/" + randomNamespaceName(10) +
		"/" + randomNamespaceName(8)

	api := th.getApi()
	err := api.Branch(src, dst)
	th.Assert(err == nil, "Failed to branch workspace: %s -> %s: %v", src, dst,
		err)

	return dst
}

// Branch existing workspace into new random name
//
// Returns the relative path of the new workspace.
func (th *TestHelper) branchWorkspace(original string) string {
	dst := th.branchWorkspaceWithoutWritePerm(original)

	api := th.getApi()
	err := api.EnableRootWrite(dst)
	th.Assert(err == nil, "Failed to enable write permission in workspace: %v",
		err)

	return dst
}

// Sync all the active workspaces
func (th *TestHelper) SyncAllWorkspaces() {
	api := th.getApi()
	err := api.SyncAll()

	th.Assert(err == nil, "Error when syncing all workspaces: %v", err)
}

func (th *TestHelper) GetWorkspaceDB() quantumfs.WorkspaceDB {
	return th.qfs.config.WorkspaceDB
}

func (th *TestHelper) SetDataStore(ds quantumfs.DataStore) {
	th.qfs.c.dataStore.durableStore = ds
}

func (th *TestHelper) GetDataStore() quantumfs.DataStore {
	return th.qfs.c.dataStore.durableStore
}

var genDataMutex sync.RWMutex
var precompGenData []byte
var genDataLast int

func GenData(maxLen int) []byte {
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

// Global test request ID incremented for all the running tests
var requestId = uint64(1000000000)

// Temporary directory for this test run
var TestRunDir string

func init() {
	TestRunDir = testutils.TestRunDir
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

type crashOnWrite struct {
	FileHandle
}

func (crash *crashOnWrite) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	panic("Intentional crash")
}

var origGC int

func PreTestRuns() {

	testutils.PreTestRuns()

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
	origGC = debug.SetGCPercent(-1)
}

func PostTestRuns() {

	// We've finished running the tests and are about to do the full logscan.
	// This create a tremendous amount of garbage, so we must enable garbage
	// collection.
	runtime.GC()
	debug.SetGCPercent(origGC)

	testutils.PostTestRuns()
}
