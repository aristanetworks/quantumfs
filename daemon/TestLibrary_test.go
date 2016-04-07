// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test library

import "io/ioutil"
import "os"
import "runtime"
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
	if th.server != nil {
		if err := th.server.Unmount(); err != nil {
			th.t.Fatalf("Failed to unmount quantumfs instance: %v", err)
		}
	}

	if th.mountPath != "" {
		if err := os.RemoveAll(th.mountPath); err != nil {
			th.t.Fatalf("Failed to cleanup temporary mount point: %v", err)
		}
	}
}

// This helper is more of a namespacing mechanism than a coherent object
type testHelper struct {
	t         *testing.T
	testName  string
	qfs       *QuantumFs
	mountPath string
	server    *fuse.Server
}

func (th *testHelper) defaultConfig() QuantumFsConfig {
	mountPath, err := ioutil.TempDir("", "quantumfsTest")
	if err != nil {
		th.t.Fatalf("Unable to create temporary mount point: %v", err)
	}

	th.mountPath = mountPath
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
	server, err := fuse.NewServer(quantumfs, config.MountPath, &mountOptions)
	if err != nil {
		th.t.Fatalf("Failed to create quantumfs instance: %v", err)
	}

	th.server = server
	go server.Serve()
}
