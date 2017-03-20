// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspaceroot type

import "os"
import "syscall"
import "testing"
import "github.com/aristanetworks/quantumfs"

func TestWorkspaceRootApiAccess(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		// fix the api path as _null/_null/null/api so that we can verify
		// that api files in workspaceroot are really functional
		workspace := test.nullWorkspace()
		apiPath := workspace + "/" + quantumfs.ApiPath

		stat, err := os.Lstat(apiPath)
		test.assert(err == nil,
			"List api file error%v,%v", err, stat)
		stat_t := stat.Sys().(*syscall.Stat_t)
		test.assert(stat_t.Ino == quantumfs.InodeIdApi,
			"Wrong Inode number for api file")

		src := "_null/_null/null"
		dst := "wsrtest/wsrtest/wsrtest"
		api := quantumfs.NewApiWithPath(apiPath)
		assert(api != nil, "Api nil")
		err = api.Branch(src, dst)
		test.assert(err == nil,
			"Error branching with api in nullworkspace:%v", err)
		api.Close()
	})
}
