// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the basic namespace listing functionality

import "io/ioutil"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"

func TestTypespaceListing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		var stat syscall.Stat_t
		err := syscall.Stat(test.absPath(quantumfs.ApiPath), &stat)
		test.Assert(err == nil, "Error getting api stat data: %v", err)
		test.Assert(stat.Ino == quantumfs.InodeIdApi,
			"api file has incorrect inode number %d", stat.Ino)

		entries, err := ioutil.ReadDir(test.absPath(""))
		test.Assert(err == nil, "Couldn't read root listing")
		test.Assert(len(entries) == 2,
			"Incorrect number of entries in empty root: %d",
			len(entries))
	})
}

func TestNamespaceListing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		entries, err :=
			ioutil.ReadDir(test.absPath(quantumfs.NullSpaceName))
		test.Assert(err == nil, "Couldn't read typespace listing")
		test.Assert(len(entries) == 1,
			"Incorrect number of entries in null typespace: %d",
			len(entries))
	})
}

func TestWorkspaceListing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		entries, err :=
			ioutil.ReadDir(test.absPath(quantumfs.NullSpaceName))
		test.Assert(err == nil, "Couldn't read namespace listing")
		test.Assert(len(entries) == 1,
			"Incorrect number of entries in null namespace: %d",
			len(entries))
	})
}

func TestNullWorkspaceListing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		path := test.newWorkspace()

		entries, err := ioutil.ReadDir(path)
		test.Assert(err == nil, "Couldn't read workspace listing")
		// The only file existing in nullworkspace is the api file
		test.Assert(len(entries) == 1,
			"Incorrect number of entries in null workspace: %d",
			len(entries))
	})
}

func checkNlink(test *testHelper, path string, expectedCountGetAttr uint64,
	expectedCountReadDirPlus uint64) {

	// Trigger the GetAttr code path for determining nlink
	var stat syscall.Stat_t
	err := syscall.Stat(test.absPath(path), &stat)
	test.AssertNoErr(err)

	test.Assert(stat.Nlink == expectedCountGetAttr,
		"%s has incorrect nlink %d != %d", path, stat.Nlink,
		expectedCountGetAttr)

	// Trigger the ReadDirPlus code path for determining nlink
	_, err = ioutil.ReadDir(test.absPath(path))

	err = syscall.Stat(test.absPath(path), &stat)
	test.AssertNoErr(err)

	test.Assert(stat.Nlink == expectedCountReadDirPlus,
		"%s has incorrect nlink %d != %d", path, stat.Nlink,
		expectedCountReadDirPlus)
}

func TestListingNlinkValues(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaces := [...]string{
			"a1/b1/c1",
			"a1/b2/c1",
			"a1/b2/c2",
			"a2/b3/c1",
		}

		api := test.getApi()

		for _, name := range workspaces {
			err := api.Branch(test.nullWorkspaceRel(), name)
			test.AssertNoErr(err)
		}

		checkNlink(test, "a1", 4, 4)
		checkNlink(test, "a1/.", 4, 4)
		checkNlink(test, "a1/b1/..", 4, 4)
		checkNlink(test, "a1/b1", 3, 3)
		checkNlink(test, "a1/b1/.", 3, 3)
		checkNlink(test, "a1/b2", 4, 4)
		checkNlink(test, "a1/b2/c1/..", 4, 4)

		// WorkspaceRoots only accurately compute nlink when accessing the
		// Inode directly and not just its parent.
		checkNlink(test, "a1/b2/c1", 29, 2)
		checkNlink(test, "a1/b2/c1/.", 2, 2)
	})
}
