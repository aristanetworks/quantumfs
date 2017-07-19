// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library for daemon package

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

const fusectlPath = "/sys/fs/fuse/"

type QuantumFsTest func(test *TestHelper)

// TestHelper holds the variables important to maintain the state of testing
// in a package which intends to use a QFS instance. daemon.TestHelper will
// need to be embedded in that package's testHelper.
type TestHelper struct {
	testutils.TestHelper
	qfs             *QuantumFs
	qfsInstances    []*QuantumFs
	qfsWait         sync.WaitGroup
	fuseConnections []int
	api             quantumfs.Api
}

func abortFuse(th *TestHelper) {
	for _, connection := range th.fuseConnections {
		if connection == 0 {
			// Nothing to abort
			continue
		}

		// Forcefully abort the filesystem so it can be unmounted
		th.T.Logf("Aborting FUSE connection %d", connection)
		path := fmt.Sprintf("%s/connections/%d/abort", fusectlPath,
			connection)
		abort, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			// We cannot abort so we won't terminate. We are
			// truly wedged.
			th.Log("ERROR: Failed to abort FUSE connection (open) "+
				"err: %s", err.Error())
		} else if _, err := abort.Write([]byte("1")); err != nil {
			th.Log("ERROR: Failed to abort FUSE connection (write) "+
				"err: %s", err.Error())
		}

		abort.Close()
	}
}

// EndTest cleans up the testing environment after the test has finished
func (th *TestHelper) EndTest() {
	exception := recover()

	if th.api != nil {
		th.api.Close()
	}

	for _, qfs := range th.qfsInstances {
		if qfs != nil && qfs.server != nil {
			if exception != nil {
				th.T.Logf("Failed with exception, forcefully "+
					"unmounting: %v", exception)
				th.Log("Failed with exception, forcefully "+
					"unmounting: %v", exception)
				abortFuse(th)
			}

			if err := qfs.server.Unmount(); err != nil {
				th.Log("ERROR: Failed to unmount quantumfs instance")
				th.Log("Are you leaking a file descriptor?: %s",
					err.Error())

				abortFuse(th)
				runtime.GC()
				if err := qfs.server.Unmount(); err != nil {
					th.Log("ERROR: Failed to unmount quantumfs "+
						"after aborting: %s", err.Error())
				}
			}
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
		CacheSize:        64 * 1024 * 1024,
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
func (th *TestHelper) serveSafely(qfs *QuantumFs) {
	defer func(th *TestHelper) {
		exception := recover()
		if exception != nil {
			for _, connection := range th.fuseConnections {
				if connection != 0 {
					abortFuse(th)
				}
				th.T.Fatalf("FUSE panic'd: %v", exception)
			}
		}
	}(th)

	var mountOptions = fuse.MountOptions{
		Name: th.TestName,
	}

	// Ensure that, since we're in a test, we only sync when syncAll is called.
	// Otherwise, we shouldn't ever need to flush.
	qfs.skipFlush = true

	th.qfsWait.Add(1)
	defer th.qfsWait.Done()
	th.AssertNoErr(qfs.Serve(mountOptions))
}

func (th *TestHelper) startQuantumFs(config QuantumFsConfig) {
	if err := utils.MkdirAll(config.CachePath, 0777); err != nil {
		th.T.Fatalf("Unable to setup test ramfs path")
	}

	instanceNum := len(th.qfsInstances) + 1

	th.Log("Instantiating quantumfs instance %d...", instanceNum)
	qfs := NewQuantumFsLogs(config, th.Logger)
	th.qfsInstances = append(th.qfsInstances, qfs)

	th.Log("Waiting for QuantumFs instance to start...")

	go th.serveSafely(qfs)

	connection := findFuseConnection(th.TestCtx(), config.MountPath)
	th.fuseConnections = append(th.fuseConnections, connection)
	th.Assert(connection != -1, "Failed to find mount")
	th.Log("QuantumFs instance started")

	if instanceNum == 1 {
		th.qfs = qfs
	}
}

func (th *TestHelper) waitForQuantumFsToFinish() {
	th.qfsWait.Wait()
}

func (th *TestHelper) RestartQuantumFs() error {
	config := th.qfsInstances[0].config
	th.api.Close()

	for _, qfs := range th.qfsInstances {
		err := qfs.server.Unmount()
		if err != nil {
			return err
		}
	}

	th.waitForQuantumFsToFinish()
	th.qfsInstances = nil
	th.fuseConnections = nil
	th.startQuantumFs(config)
	return nil
}

func (th *TestHelper) getApi() quantumfs.Api {
	if th.api != nil {
		return th.api
	}

	api, err := quantumfs.NewApiWithPath(th.AbsPath(quantumfs.ApiPath))
	th.Assert(err == nil, "Error getting api: %v", err)
	th.api = api
	return th.api
}

func (th *TestHelper) putApi() {
	if th.api != nil {
		th.api.Close()
	}
	th.api = nil
}

func (th *TestHelper) getUniqueApi(fdPath string) quantumfs.Api {
	api, err := quantumfs.NewApiWithPath(fdPath)
	th.Assert(err == nil, "Error getting unique api: %v", err)
	return api
}

// Make the given path absolute to the mount root
func (th *TestHelper) AbsPath(path string) string {
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
	return th.AbsPath(th.nullWorkspaceRel())
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

	return th.AbsPath(dst)
}

// Create a new workspace to test within
//
// Returns the absolute path of the workspace
func (th *TestHelper) NewWorkspace() string {
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

// CreateTestDirs makes the required directories for the test.
// These directories are inside TestRunDir

func (th *TestHelper) CreateTestDirs() {
	mountPath := th.TempDir + "/mnt"
	utils.MkdirAll(mountPath, 0777)
	th.Log("Using mountpath %s", mountPath)

	utils.MkdirAll(th.TempDir+"/mnt2", 0777)
	utils.MkdirAll(th.TempDir+"/ether", 0777)
}

var genDataMutex utils.DeferableMutex
var precompGenData []byte
var genDataLast int

func GenData(maxLen int) []byte {
	defer genDataMutex.Lock().Unlock()

	if maxLen > len(precompGenData) {
		// we need to expand the array

		for len(precompGenData) <= maxLen {
			precompGenData = append(precompGenData,
				strconv.Itoa(genDataLast)...)
			genDataLast++
		}
	}

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
func (th *TestHelper) TestCtx() *ctx {
	return th.dummyReq(qlog.TestReqId)
}

//only to be used for some testing - not all functions will work with this
func (th *TestHelper) dummyReq(request uint64) *ctx {
	requestCtx := &ctx{
		Ctx: quantumfs.Ctx{
			Qlog:      th.Logger,
			RequestId: request,
		},
	}

	if th.qfs != nil {
		requestCtx.qfs = th.qfs
		requestCtx.config = th.qfs.c.config
		requestCtx.workspaceDB = th.qfs.c.workspaceDB
		requestCtx.dataStore = th.qfs.c.dataStore
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
