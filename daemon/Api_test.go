// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various Api calls

import "fmt"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"

func TestWorkspaceBranching(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		// First branch the null workspace
		src := test.nullWorkspaceRel()
		dst := "work/apitest/a"
		err := api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Branch the branch to have a writeable workspace
		src = dst
		dst = "work/apitest/b"
		err = api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Then create a file
		testFilename := test.absPath(dst + "/" + "test")
		fd, _ := os.Create(testFilename)
		fd.Close()
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)

		// Ensure the first branched workspace wasn't modified
		testFilename = test.absPath(src + "/" + "test")
		err = syscall.Stat(testFilename, &stat)
		test.assert(err != nil, "Original workspace was modified")
	})
}

func testApiAccessList(test *testHelper, size int, filename string,
	concurrent bool) {

	workspace := test.newWorkspace()
	accessList, expectedSize := generateFile(test, size, workspace, filename)

	api := test.getApi()
	relpath := test.relPath(workspace)

	if concurrent {
		workspace2 := test.newWorkspace()
		size2 := 100
		filename2 := "concurrentconcurrentconcurrentconcurrentconcurrent"
		generateFile(test, size2, workspace2, filename2)

		relpath2 := test.relPath(workspace2)
		go func() {
			path := workspace2 + "/" + filename2 + "0"
			api2 := test.getNewApi(path)
			test.assert(api != api2,
				"Error getting the same file descriptor")
			defer api2.Close()
			api2.GetAccessed(relpath2)
		}()
	}

	responselist, err := api.GetAccessed(relpath)
	test.assert(err == nil, "Error getting accessList with api %v", err)

	test.assert(mapSize(responselist) == expectedSize,
		"Error getting unequal sizes %d != %d",
		mapSize(responselist), expectedSize)

	test.assertAccessList(accessList, responselist, "Error two maps different")
}

func generateFile(test *testHelper, size int, workspace,
	filename string) (map[string]bool, int) {

	accessList := make(map[string]bool)
	expectedSize := 0
	for i := 0; i < size; i++ {
		filename := fmt.Sprintf("/%s%d", filename, i)
		expectedSize += len(filename)
		path := workspace + filename
		fd, err := syscall.Creat(path, 666)
		test.assert(err == nil, "Create file error: %v at %s",
			err, filename)
		accessList[filename] = true
		syscall.Close(fd)
	}

	test.assert(len(accessList) == size, "Fail creating correct "+
		"accesslist with size of %d", len(accessList))

	wsrlist := test.getAccessList(workspace)
	test.assertAccessList(accessList, wsrlist, "Error two maps different")

	return accessList, expectedSize
}

func mapSize(list map[string]bool) int {
	size := 0
	for key, _ := range list {
		size += len(key)
	}

	return size
}

func TestApiAccessListEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testApiAccessList(test, 0, "", false)
	})
}

func TestApiAccessListLargeSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testApiAccessList(test, 200, "testfiletestfiletestfiletestfile"+
			"testfiletestfiletestfiletestfiletestfiletestfile", false)
	})
}

func TestApiAccessListConcurrent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testApiAccessList(test, 500, "sample", true)
	})
}

func TestApiClearAccessList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		api := test.getApi()

		relpath := test.relPath(workspace)
		err = api.ClearAccessed(relpath)
		test.assert(err == nil,
			"Error clearing accessList with api")
		accessList = make(map[string]bool)
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error maps not clear")
	})
}

func getExtendedKeyHelper(test *testHelper, dst string, type_ string) []byte {
	key := make([]byte, quantumfs.ExtendedKeyLength)
	sz, err := syscall.Getxattr(dst, quantumfs.XAttrTypeKey, key)
	test.assert(err == nil && sz == quantumfs.ExtendedKeyLength,
		"Error getting the key of %s: %v with a size of %d", type_, err, sz)
	return key
}

func ApiInsertInodeTest(test *testHelper, uid uint32, gid uint32) {
	api := test.getApi()

	// Create the source and the target workspace
	workspaceSrc := test.newWorkspace()
	workspaceDst := test.newWorkspace()
	dst := test.relPath(workspaceDst)

	dirName := workspaceSrc + "/test/a"
	dirName1 := dirName + "/b"
	testFilename := dirName1 + "/test"
	linkFilename := workspaceSrc + "/link"
	spFilename := workspaceSrc + "/pipe"

	var PermissionA, PermissionB, expectedMode uint32
	PermissionA = syscall.S_IXUSR | syscall.S_IWGRP | syscall.S_IROTH
	PermissionB = syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO

	err := os.MkdirAll(dirName1, os.FileMode(PermissionA))
	test.assert(err == nil, "Error creating directories: %v", err)

	fd, err := syscall.Creat(testFilename, PermissionB)
	test.assert(err == nil, "Error creating a small file: %v", err)
	syscall.Close(fd)

	err = syscall.Symlink(testFilename, linkFilename)
	test.assert(err == nil, "Error creating a symlink: %v", err)

	expectedMode = syscall.S_IFIFO | syscall.S_IRWXU
	err = syscall.Mknod(spFilename, expectedMode,
		0x12345678)
	test.assert(err == nil, "Error creating pipe")

	// get the key from a file
	keyF := getExtendedKeyHelper(test, testFilename, "file")

	// get the key from a directory
	keyD := getExtendedKeyHelper(test, dirName1, "directory")

	// get the key from a Symlink
	keyS := make([]byte, quantumfs.ExtendedKeyLength)
	sz, err, keyS := lGetXattr(linkFilename, quantumfs.XAttrTypeKey,
		quantumfs.ExtendedKeyLength)
	test.assert(err == nil && sz == quantumfs.ExtendedKeyLength,
		"Error getting the Key of symlink: %v with a size of %d",
		err, sz)

	// get the key from a pipe
	keyP := getExtendedKeyHelper(test, spFilename, "pipe")

	dirNameD := test.absPath(dst + "/test/a")
	err = os.MkdirAll(dirNameD, os.FileMode(PermissionA))
	test.assert(err == nil, "Error creating target directories: %v", err)

	// Ensure the workspace root cannot be duplicated
	err = api.InsertInode(dst, string(keyF), PermissionA, uid, gid)
	test.assert(err != nil,
		"Unexpected success duplicating workspace root")

	// Ensure the non-existing intermediate Inode not be created
	err = api.InsertInode(dst+"/nonExist/b", string(keyF),
		PermissionA, uid, gid)
	test.assert(err != nil,
		"Unexpected success creating non-existing intermediate"+
			" Inode")

	// Ensure the target node does not exist
	err = api.InsertInode(dst+"/test/a", string(keyF),
		PermissionA, uid, gid)
	test.assert(err != nil,
		"Error having the target node already")

	// Duplicate the file in the given path
	err = api.InsertInode(dst+"/test/a/file", string(keyF),
		PermissionA, uid, gid)
	test.assert(err == nil,
		"Error duplicating a file to target workspace: %v", err)

	var stat syscall.Stat_t
	err = syscall.Stat(workspaceDst+"/test/a/file", &stat)
	test.assert(err == nil, "Error get status of a file: %v", err)

	// check the mode of file
	expectedMode = syscall.S_IFREG | PermissionA
	test.assert(stat.Mode == expectedMode,
		"File mode incorrect. Expected %x got %x",
		expectedMode, stat.Mode)

	var expectedUid uint32
	if uid < quantumfs.UIDUser {
		expectedUid = uint32(uid)
	} else {
		expectedUid = quantumfs.UniversalUID
	}

	var expectedGid uint32
	if gid < quantumfs.GIDUser {
		expectedGid = uint32(gid)
	} else {
		expectedGid = quantumfs.UniversalGID
	}

	test.assert(stat.Uid == expectedUid, "uid doesn't match %d %d",
		stat.Uid, expectedUid)
	test.assert(stat.Gid == expectedGid, "gid doesn't match %d %d",
		stat.Gid, expectedGid)

	// Duplicate the directory in the given path
	err = api.InsertInode(dst+"/test/a/dirtest", string(keyD),
		PermissionA, uid, gid)
	test.assert(err == nil,
		"Error duplicating a directory to target workspace: %v",
		err)

	err = syscall.Stat(workspaceDst+"/test/a/dirtest", &stat)
	test.assert(err == nil, "Error getting status of directory: %v",
		err)

	// check the mode of directory
	expectedMode = syscall.S_IFDIR | PermissionA
	test.assert(stat.Mode == expectedMode,
		"Directory mode incorrect. Expected %x got %x",
		expectedMode, stat.Mode)

	// testing the file inside of the directory
	err = syscall.Stat(workspaceDst+"/test/a/dirtest/test", &stat)
	test.assert(err == nil, "Error getting status of child file: %v",
		err)

	// Ensure the no intermediate inode is a file
	err = api.InsertInode(dst+"/test/a/dirtest/test", string(keyF),
		PermissionA, uid, gid)
	test.assert(err != nil,
		"Unexpected success creating a file inside of a file")

	// check the child is a file
	expectedMode = syscall.S_IFREG | PermissionB
	test.assert(stat.Mode == expectedMode,
		"Directory's file mode incorrect. Expected %x got %x",
		expectedMode, stat.Mode)

	// Ensure the symlink in the given path
	err = api.InsertInode(dst+"/symlink", string(keyS),
		PermissionB, uid, gid)
	test.assert(err == nil,
		"Error duplicating a symlink to workspace: %v", err)

	err = syscall.Lstat(workspaceDst+"/symlink", &stat)
	test.assert(err == nil,
		"Error getting status of Symlink: %v", err)

	expectedMode = syscall.S_IFLNK | PermissionB
	test.assert(stat.Mode == expectedMode,
		"Symlink mode incorrect. Expected %x got %x %d",
		expectedMode, stat.Mode, stat.Size)

	// Ensure the pipe file in the given path
	err = api.InsertInode(dst+"/Pipe", string(keyP),
		PermissionB, uid, gid)
	test.assert(err == nil,
		"Error duplicating a pipe file to workspace: %v", err)

	err = syscall.Stat(workspaceDst+"/Pipe", &stat)
	test.assert(err == nil,
		"Error getting status of Pipe file: %v", err)

	expectedMode = syscall.S_IFIFO | PermissionB
	test.assert(stat.Mode == expectedMode,
		"Pipe file mode incorrect. Expected %o got %o %d",
		expectedMode, stat.Mode, stat.Size)
}

func TestApiInsertInode(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ApiInsertInodeTest(test, 0, 0)
	})
}

func TestApiInsertInodeAsUser(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ApiInsertInodeTest(test, 10100, 10999)
	})
}

func TestApiNoRequestBlockingRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api, err := os.Open(test.absPath(quantumfs.ApiPath))
		test.assert(err == nil, "Error opening api file: %v", err)
		defer api.Close()

		buf := make([]byte, 0, 256)
		n, err := api.Read(buf)
		test.assert(n == 0, "Wrong number of bytes read: %d", n)
	})
}

func TestApiNoRequestNonBlockingRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api, err := os.OpenFile(test.absPath(quantumfs.ApiPath),
			syscall.O_NONBLOCK, 0)
		test.assert(err == nil, "Error opening api file: %v", err)
		defer api.Close()

		buf := make([]byte, 0, 256)
		n, err := api.Read(buf)
		test.assert(n == 0, "Wrong number of bytes read: %d", n)
		test.assert(err.(*os.PathError).Err == syscall.EAGAIN,
			"Non-blocking read api without requests error:%v", err)
	})
}
