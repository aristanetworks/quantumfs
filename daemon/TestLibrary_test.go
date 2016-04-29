// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library

import "fmt"
import "io/ioutil"
import "os"
import "runtime"
import "sync/atomic"
import "testing"

import "arista.com/quantumfs"
import "arista.com/quantumfs/processlocal"

import "github.com/hanwen/go-fuse/fuse"

// startTest is a helper which configures the testing environment
func startTest(t *testing.T) testHelper {
	t.Parallel()

	testPc, _, _, _ := runtime.Caller(1)
	testName := runtime.FuncForPC(testPc).Name()
	return testHelper{
		t:        t,
		testName: testName,
	}
}

// endTest cleans up the testing environment after the test has finished
func (th *testHelper) endTest() {
	if th.api != nil {
		th.api.Close()
	}

	if th.server != nil {
		if err := th.server.Unmount(); err != nil {
			th.t.Fatalf("Failed to unmount quantumfs instance: %v", err)
		}
	}

	if th.tempDir != "" {
		if err := os.RemoveAll(th.tempDir); err != nil {
			th.t.Fatalf("Failed to cleanup temporary mount point: %v",
				err)
		}
	}
}

// This helper is more of a namespacing mechanism than a coherent object
type testHelper struct {
	t        *testing.T
	testName string
	qfs      *QuantumFs
	tempDir  string
	server   *fuse.Server
	api      *quantumfs.Api
}

func (th *testHelper) defaultConfig() QuantumFsConfig {
	tempDir, err := ioutil.TempDir("", "quantumfsTest")
	if err != nil {
		th.t.Fatalf("Unable to create temporary mount point: %v", err)
	}

	th.tempDir = tempDir
	mountPath := tempDir + "/mnt"

	os.Mkdir(mountPath, 0777)
	th.t.Log("Using mountpath", mountPath)

	config := QuantumFsConfig{
		CachePath:        "",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(),
		DurableStore:     processlocal.NewDataStore(),
	}
	return config
}

func (th *testHelper) startDefaultQuantumFs() {
	config := th.defaultConfig()
	th.startQuantumFs(config)
}

func (th *testHelper) startQuantumFs(config QuantumFsConfig) {
	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "cluster",
		Name:          th.testName,
	}

	quantumfs := NewQuantumFs(config)
	th.qfs = quantumfs.(*QuantumFs)
	server, err := fuse.NewServer(quantumfs, config.MountPath, &mountOptions)
	if err != nil {
		th.t.Fatalf("Failed to create quantumfs instance: %v", err)
	}

	th.server = server
	go server.Serve()
}

func (th *testHelper) getApi() *quantumfs.Api {
	if th.api != nil {
		return th.api
	}

	th.api = quantumfs.NewApiWithPath(th.relPath(quantumfs.ApiPath))
	return th.api
}

// Make the given path relative to the mount root
func (th *testHelper) relPath(path string) string {
	return th.tempDir + "/mnt/" + path
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

// Retrieve the rootId of the given workspace
func (th *testHelper) workspaceRootId(namespace string,
	workspace string) quantumfs.ObjectKey {

	return th.qfs.c.workspaceDB.Workspace(namespace, workspace)
}

// Global test request ID incremented for all the running tests
var requestId = uint64(1000000000)

// Produce a request specific ctx variable to use for quantumfs internal calls
func (th *testHelper) newCtx() *ctx {
	reqId := atomic.AddUint64(&requestId, 1)
	fmt.Println("Allocating request %d to test %s", reqId, th.testName)
	return th.qfs.c.req(reqId)
}

// assert the condition is true. If it is not true then fail the test with the given
// message
func (th *testHelper) assert(condition bool, format string, args ...interface{}) {
	if !condition {
		th.endTest()
		th.t.Fatalf(format, args...)
	}
}
