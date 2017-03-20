// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the basic namespace listing functionality

import "io/ioutil"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"

func TestTypespaceListing(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		var stat syscall.Stat_t
		err := syscall.Stat(test.absPath(quantumfs.ApiPath), &stat)
		test.assert(err == nil, "Error getting api stat data: %v", err)
		test.assert(stat.Ino == quantumfs.InodeIdApi,
			"api file has incorrect inode number %d", stat.Ino)

		entries, err := ioutil.ReadDir(test.absPath(""))
		test.assert(err == nil, "Couldn't read root listing")
		test.assert(len(entries) == 2,
			"Incorrect number of entries in empty root: %d",
			len(entries))
	})
}

func TestNamespaceListing(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		entries, err :=
			ioutil.ReadDir(test.absPath(quantumfs.NullTypespaceName))
		test.assert(err == nil, "Couldn't read typespace listing")
		test.assert(len(entries) == 1,
			"Incorrect number of entries in null typespace: %d",
			len(entries))
	})
}

func TestWorkspaceListing(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		entries, err :=
			ioutil.ReadDir(test.absPath(quantumfs.NullNamespaceName))
		test.assert(err == nil, "Couldn't read namespace listing")
		test.assert(len(entries) == 1,
			"Incorrect number of entries in null namespace: %d",
			len(entries))
	})
}

func TestNullWorkspaceListing(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		path := test.newWorkspace()

		entries, err := ioutil.ReadDir(path)
		test.assert(err == nil, "Couldn't read workspace listing")
		// The only file existing in nullworkspace is the api file
		test.assert(len(entries) == 1,
			"Incorrect number of entries in null workspace: %d",
			len(entries))
	})
}
