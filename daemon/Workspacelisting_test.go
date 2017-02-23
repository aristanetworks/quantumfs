// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspacelisting type

// import "os"
import "syscall"
import "testing"
import "github.com/aristanetworks/quantumfs"

func verifyResult(c *ctx, test *testHelper, name string, space string,
	expect bool, inodeMap *map[string]InodeId) InodeId {

	id, exists := (*inodeMap)[name]
	test.assert(exists, "Fail to get the inodeId of %s", space)
	inode, _ := test.qfs.getInode(c, id)
	if expect {
		test.assert(inode != nil,
			"The %s should be instantiated", space)
	} else {
		test.assert(inode == nil,
			"The %s should be uninstantiated", space)
	}

	return id
}

func TestWorkspacelistingInstantiateOnDemand(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.newCtx()
		tslInode, _ := test.qfs.getInode(c, quantumfs.InodeIdRoot)
		tsl := tslInode.(*TypespaceList)

		workspace, type_, name_, work_ := test.newWorkspaceWithNames()
		_, exists := tsl.typespacesByName[type_]
		test.assert(!exists,
			"Error getting a non-existing inodeId of typespace")

		// Creating a file in the new workspace can trigger updateChildren
		// in workspacelisting. Map within will be updated, so inodes in
		// the proceeding workspace will be valid right now.
		workspace1 := test.newWorkspace()
		testFilename := workspace1 + "/" + "test"
		err := syscall.Mkdir(testFilename, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		// Verify that the typespace has been assigned an id to but not
		// instantiated yet. If the inode is not created, there is no
		// need to verify its descendents: namespace and worksapce.
		verifyResult(c, test, type_, "typespace",
			false, &tsl.typespacesByName)

		// Instantiate the three inodes and verify the existence
		testFilename = workspace + "/" + "test"
		err = syscall.Mkdir(testFilename, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		nslId := verifyResult(c, test, type_, "typespace",
			true, &tsl.typespacesByName)
		nslInode, _ := test.qfs.getInode(c, nslId)
		nsl := nslInode.(*NamespaceList)

		wslId := verifyResult(c, test, name_, "namespace",
			true, &nsl.namespacesByName)
		wslInode, _ := test.qfs.getInode(c, wslId)
		wsl := wslInode.(*WorkspaceList)

		verifyResult(c, test, work_, "workspace",
			true, &wsl.workspacesByName)
	})
}
