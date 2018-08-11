// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library for daemon package

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
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
	qfsWait         sync.WaitGroup
	fuseConnections []int

	qfsInstances      []*QuantumFs
	qfsInstancesMutex utils.DeferableMutex

	// runtimeMutex protects api and finished fields
	runtimeMutex utils.DeferableMutex
	api          quantumfs.Api
	finished     bool
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
	newInode, release := th.qfs.inodeNoInstantiate(&th.qfs.c, inodeNum)
	// For now, we don't care too much about the inode being uninstantiated
	// early during a test
	release()
	return newInode
}

func (th *TestHelper) GetRecord(path string) quantumfs.ImmutableDirectoryRecord {
	inode := th.getInode(path)

	parentId := func() InodeId {
		lock := inode.getParentLock()
		defer (*lock).RLock().RUnlock()
		return inode.parentId_()
	}()

	parent, release := th.qfs.inodeNoInstantiate(&th.qfs.c, parentId)
	defer release()
	parentDir := asDirectory(parent)

	defer parentDir.childRecordLock.Lock().Unlock()
	return parentDir.getRecordChildCall_(&th.qfs.c, inode.inodeNum())
}

func logFuseWaiting(prefix string, th *TestHelper) {
	for _, connection := range th.fuseConnections {
		if connection == 0 {
			// There is no connection to log
			continue
		}

		buf := make([]byte, 100)

		path := fmt.Sprintf("%s/connections/%d/waiting", fusectlPath,
			connection)
		waiting, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			if os.IsNotExist(err) {
				// If the fuse connection doesn't exist, then this
				// particular connection has successfully terminated.
				continue
			}
			th.Log("ERROR: Open FUSE connection (%d) failed "+
				"waiting err: %s", connection, err.Error())
		} else if _, err := waiting.Read(buf); err != nil {
			th.Log("ERROR: Read FUSE connection (%d) failed "+
				"waiting err: %s", connection, err.Error())
		} else {
			str := bytes.TrimRight(buf, "\u0000\n")
			th.Log("%s: there are %s pending requests on connection %d",
				prefix, str, connection)
		}
		waiting.Close()
	}
}

func (th *TestHelper) AbortFuse() {
	for _, connection := range th.fuseConnections {
		if connection <= 0 {
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

	defer th.ShutdownLogger()
	defer func() {
		if th.qfs != nil {
			close(th.qfs.toBeReleased)
		}
	}()

	th.finishApi()
	th.putApi()

	var qfsInstances []*QuantumFs
	func() {
		defer th.qfsInstancesMutex.Lock().Unlock()
		qfsInstances, th.qfsInstances = th.qfsInstances, nil
	}()

	for _, qfs := range qfsInstances {
		if qfs != nil && qfs.server != nil {
			if exception != nil {
				th.T.Logf("Failed with exception, forcefully "+
					"unmounting: %v", exception)
				th.Log("Failed with exception, forcefully "+
					"unmounting: %v", exception)
				th.AbortFuse()
			}
			logFuseWaiting("Before unmount", th)
			var err error
			for i := 0; i < 10; i++ {
				err = qfs.server.Unmount()
				if err == nil {
					break
				}
				th.Log("umount try %d failed with %s, retrying",
					i+1, err.Error())
				time.Sleep(time.Millisecond)
			}
			if err != nil {
				th.Log("ERROR: Failed to unmount quantumfs instance")
				th.Log("Are you leaking a file descriptor?: %s",
					err.Error())
				logFuseWaiting("After unmount failure", th)

				th.AbortFuse()
				runtime.GC()
				logFuseWaiting("After aborting fuse", th)
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

		testFailed := false
		if th.T != nil {
			testFailed = th.Logscan()
		}
		if !testFailed {
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
		DirtyFlushDelay:  Duration{30 * time.Second},
		MemLogBytes:      uint64(qlog.DefaultMmapSize),
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(""),
		DurableStore:     processlocal.NewDataStore(""),
		MagicOwnership:   true,
	}
	return config
}

// StartDefaultQuantumFs start the qfs daemon with default config.
func (th *TestHelper) StartDefaultQuantumFs(startChan chan struct{}) {
	config := th.defaultConfig()
	th.startQuantumFs(config, startChan, false)
}

func (th *TestHelper) etherFilesystemConfig() QuantumFsConfig {
	mountPath := th.TempDir + "/mnt"

	datastorePath := th.TempDir + "/ether"
	datastore, err := thirdparty_backends.ConnectDatastore("ether.filesystem",
		datastorePath)
	th.AssertNoErr(err)

	config := QuantumFsConfig{
		CachePath:        th.TempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  Duration{30 * time.Second},
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(""),
		DurableStore:     datastore,
	}
	return config
}

func (th *TestHelper) StartEtherFileQuantumFs(startChan chan struct{}) {
	th.startQuantumFs(th.etherFilesystemConfig(), startChan, false)
}

func (th *TestHelper) StartQuantumFsWithWsdb(wsdb quantumfs.WorkspaceDB,
	startChan chan struct{}) {

	config := th.defaultConfig()
	config.WorkspaceDB = wsdb
	th.startQuantumFs(config, startChan, false)
}

// If the filesystem panics, abort it and unmount it to prevent the test binary from
// hanging.
func (th *TestHelper) serveSafely(qfs *QuantumFs, startChan chan<- struct{}) {
	defer func(th *TestHelper) {
		exception := recover()
		if exception != nil {
			for _, connection := range th.fuseConnections {
				if connection != 0 {
					th.AbortFuse()
				}
				th.T.Fatalf("FUSE panic'd: %v", exception)
			}
		}
	}(th)

	var mountOptions = fuse.MountOptions{
		Name: th.TestName,
	}

	th.qfsWait.Add(1)
	defer th.qfsWait.Done()

	func() {
		defer th.qfsInstancesMutex.Lock().Unlock()
		if th.isFinished() {
			th.Log("Test already finished. Not starting qfs.")
			panic("Test has finished")
		}
		th.AssertNoErr(qfs.Mount(mountOptions))
		if startChan != nil {
			close(startChan)
		}
	}()
	qfs.Serve()
}

func (th *TestHelper) startQuantumFs(config QuantumFsConfig,
	startChan chan struct{}, logPrefix bool) {

	if err := utils.MkdirAll(config.CachePath, 0777); err != nil {
		th.T.Fatalf("Unable to setup test ramfs path")
	}

	var qfs *QuantumFs
	var instanceNum int
	func() {
		defer th.qfsInstancesMutex.Lock().Unlock()
		instanceNum = len(th.qfsInstances) + 1

		th.Log("Instantiating quantumfs instance %d...", instanceNum)
		qfs = NewQuantumFsLogs(config, th.Logger)
		if logPrefix {
			qfs.c.Ctx.Prefix = fmt.Sprintf("[%d]: ", instanceNum)
		}
		qfs.syncAllRetries = 5
		th.qfsInstances = append(th.qfsInstances, qfs)
	}()

	if th.isFinished() {
		th.Log("Test already finished. Not starting qfs.")
		return
	}

	go th.serveSafely(qfs, startChan)

	th.Log("Waiting for QuantumFs instance to start...")
	connection := findFuseConnection(th.TestCtx(), config.MountPath)
	th.fuseConnections = append(th.fuseConnections, connection)
	th.Assert(connection != -1, "Failed to find mount")
	th.Log("QuantumFs instance started")

	if startChan != nil {
		select {
		case <-time.After(3 * time.Second):
			panic("ERROR: Timed out starting the qfs server")
		case <-startChan:
		}
	}
	qfs.server.WaitMount()

	if instanceNum == 1 {
		th.qfs = qfs
	}
}

func (th *TestHelper) waitForQuantumFsToFinish() {
	th.qfsWait.Wait()
}

func (th *TestHelper) RestartQuantumFs() error {

	var config QuantumFsConfig
	err := func() error {

		defer th.qfsInstancesMutex.Lock().Unlock()

		if th.finished {
			th.Log("Test already finished. Not restarting qfs.")
			return fmt.Errorf("Test has finished")
		}
		config = th.qfsInstances[0].config

		th.putApi()

		for _, qfs := range th.qfsInstances {
			err := qfs.server.Unmount()
			if err != nil {
				return err
			}
		}

		th.waitForQuantumFsToFinish()
		th.qfsInstances = nil
		return nil
	}()
	if err != nil {
		return err
	}
	th.fuseConnections = nil
	th.startQuantumFs(config, nil, false)
	return nil
}

func (th *TestHelper) isFinished() bool {
	defer th.runtimeMutex.Lock().Unlock()
	return th.finished
}

func (th *TestHelper) getApi() quantumfs.Api {
	defer th.runtimeMutex.Lock().Unlock()
	if th.finished {
		// the test has finished, accessing the api file
		// is not safe. The caller shall panic
		return nil
	}
	if th.api != nil {
		return th.api
	}

	api, err := quantumfs.NewApiWithPath(th.AbsPath(quantumfs.ApiPath))
	th.Assert(err == nil, "Error getting api: %v", err)
	th.api = api
	return th.api
}

// Prevent any further api files to get opened
func (th *TestHelper) finishApi() {
	defer th.runtimeMutex.Lock().Unlock()
	th.finished = true
}

func (th *TestHelper) putApi() {
	defer th.runtimeMutex.Lock().Unlock()
	if th.api != nil {
		th.api.Close()
	}
	th.api = nil
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
		result += string(chars[utils.RandomNumberGenerator.Intn(len(chars))])
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

func (th *TestHelper) SyncWorkspace(workspace string) {
	th.AssertNoErr(th.getApi().SyncWorkspace(workspace))
}

func (th *TestHelper) SyncWorkspaceAsync(workspace string) chan error {
	chanErr := make(chan error)
	go func() {
		chanErr <- th.getApi().SyncWorkspace(workspace)
	}()
	return chanErr
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

// Convert an absolute workspace path to the matching WorkspaceRoot object. The same
// as Mux::getWorkspaceRoot(), the caller of this function should run Forget function
// at the end.
func (th *TestHelper) GetWorkspaceRoot(workspace string) (wsr *WorkspaceRoot,
	cleanup func()) {

	parts := strings.Split(th.RelPath(workspace), "/")
	wsr, cleanup, ok := th.qfs.getWorkspaceRoot(&th.qfs.c,
		parts[0], parts[1], parts[2])
	th.Assert(ok, "WorkspaceRoot object for %s not found", workspace)

	return wsr, cleanup
}

func (th *TestHelper) WaitForRefreshTo(workspace string, dst quantumfs.ObjectKey) {
	msg := fmt.Sprintf("Refresh to %s", dst.String())
	th.WaitFor(msg, func() bool {
		wsr, cleanup := th.GetWorkspaceRoot(workspace)
		defer cleanup()
		th.Assert(wsr != nil, "workspace root does not exist")
		th.Log("Published root is %s", wsr.publishedRootId.String())
		return wsr.publishedRootId.IsEqualTo(dst)
	})
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

func (th *TestHelper) HardlinkKeyExists(workspace string,
	key quantumfs.ObjectKey) bool {

	wsr, cleanup := th.GetWorkspaceRoot(workspace)
	defer cleanup()

	defer wsr.hardlinkTable.linkLock.RLock().RUnlock()
	for _, hardkey := range wsr.hardlinkTable.hardlinks {
		if key.IsEqualTo(hardkey.record().ID()) {
			return true
		}
	}
	return false
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

func (th *TestHelper) QfsCtx() *quantumfs.Ctx {
	return &th.qfs.c.Ctx
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
