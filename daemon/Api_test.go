// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various Api calls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

func TestWorkspaceBranchNoOtherSyncs(t *testing.T) {
	// Make sure branching a workspace does not result in any unrelated workspace
	// getting flushed unnecessarily
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		workspace0 := test.NewWorkspace()
		workspaceName0 := test.RelPath(workspace0)
		workspace1 := test.NewWorkspace()
		workspaceName1 := test.RelPath(workspace1)

		test.createFile(workspace0, "testFile0", 1000)
		test.createFile(workspace1, "testFile1", 1000)

		dst := "work/apitest/target_wsr"
		test.AssertNoErr(api.Branch(workspaceName0, dst))

		advanceMsg := "Advanced rootID for "
		test.Assert(test.TestLogContains(advanceMsg+workspaceName0),
			"workspace %s must have advanced", workspaceName0)
		test.Assert(!test.TestLogContains(advanceMsg+workspaceName1),
			"workspace %s must not have advanced", workspaceName1)
	})
}

func TestWorkspaceBranching(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		// First branch the null workspace
		src := test.nullWorkspaceRel()
		dst := "work/apitest/a"
		err := api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		// Branch the branch to have a writeable workspace
		src = dst
		dst = "work/apitest/b"
		err = api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		// Enable the write permission of this workspace
		err = api.EnableRootWrite(dst)
		test.Assert(err == nil, "Failed to enable write permission in "+
			"workspace: %v", err)

		// Then create a file
		testFilename := test.AbsPath(dst + "/" + "test")
		fd, _ := os.Create(testFilename)
		fd.Close()
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)

		// Ensure the first branched workspace wasn't modified
		testFilename = test.AbsPath(src + "/" + "test")
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err != nil, "Original workspace was modified")
	})
}

func generateFiles(test *testHelper, size int, workspace,
	filename string) (quantumfs.PathsAccessed, int) {

	accessList := quantumfs.NewPathsAccessed()
	expectedSize := 0
	for i := 0; i < size; i++ {
		filename := fmt.Sprintf("/%s%d", filename, i)
		expectedSize += len(filename)
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error: %v at %s",
			err, filename)
		accessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
	}

	test.Assert(len(accessList.Paths) == size, "Fail creating correct "+
		"accesslist with size of %d", len(accessList.Paths))

	wsrlist := test.getAccessList(workspace)
	test.assertAccessList(accessList, wsrlist, "Error two maps different")

	return accessList, expectedSize
}

func mapKeySizeSum(paths *quantumfs.PathsAccessed) int {
	size := 0
	for key, _ := range paths.Paths {
		size += len(key)
	}

	return size
}

func TestApiAccessListEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		api := test.getApi()
		relpath := test.RelPath(workspace)

		responselist, err := api.GetAccessed(relpath)
		test.Assert(err == nil, "Error getting accessList with api")

		expectedSize := 0
		test.Assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		accessList := quantumfs.NewPathsAccessed()
		test.assertAccessList(accessList, responselist,
			"Error two maps different")
	})
}

func TestApiAccessListLargeSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		filename := "testfiletestfiletestfiletestfile" +
			"testfiletestfiletestfiletestfiletestfiletestfile"

		workspace := test.NewWorkspace()
		accessList, expectedSize := generateFiles(test, 200,
			workspace, filename)

		api := test.getApi()
		relpath := test.RelPath(workspace)

		responselist, err := api.GetAccessed(relpath)
		test.Assert(err == nil, "Error getting accessList with api")

		test.Assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		test.assertAccessList(accessList, responselist,
			"Error two maps different")
	})
}

func TestApiAccessListApiFileSizeResidue(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "testfiletestfiletestfiletestfiletestfiletesti" +
			"filetestfiletestfiletestfiletestfile"

		accessList, expectedSize := generateFiles(test,
			200, workspace, filename)

		api := test.getApi()
		relpath := test.RelPath(workspace)

		responselist, _ := api.GetAccessed(relpath)
		queueSize1 := atomic.LoadInt64(&test.qfs.apiFileSize)
		test.Assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		test.assertAccessList(accessList, responselist,
			"Error two maps different")

		const ApiFileHandleNum = 1
		test.qfs.setFileHandle(&test.qfs.c, ApiFileHandleNum, nil)
		queueSize2 := atomic.LoadInt64(&test.qfs.apiFileSize)
		test.Assert(queueSize1 >= int64(expectedSize) && queueSize2 == 0,
			"The apiFileSize: %d %d, the actual response size: %d)",
			queueSize1, queueSize2, expectedSize)
	})
}

func TestApiAccessListConcurrent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		size := 100
		filename := "samplesamplesamplesamplesample" +
			"samplesamplesamplesamplesample"
		workspace := test.NewWorkspace()
		accessList, expectedSize := generateFiles(test,
			size, workspace, filename)

		workspace2 := test.NewWorkspace()

		filename2 := "concurrentconcurrentconcurrent" +
			"concurrentconcurrent"
		accessList2, expectedSize2 := generateFiles(test,
			size, workspace2, filename2)

		fd1, err := os.OpenFile(workspace+"/api",
			os.O_RDWR|syscall.O_DIRECT, 0)
		defer fd1.Close()
		test.AssertNoErr(err)

		fd2, err := os.OpenFile(workspace2+"/api",
			os.O_RDWR|syscall.O_DIRECT, 0)
		defer fd2.Close()
		test.AssertNoErr(err)

		writeRequest := func(wsr string, fd *os.File) {
			cmd := quantumfs.AccessedRequest{
				CommandCommon: quantumfs.CommandCommon{
					CommandId: quantumfs.CmdGetAccessed},
				WorkspaceRoot: test.RelPath(wsr),
			}
			cmdBuf, err := json.Marshal(cmd)
			test.AssertNoErr(err)
			test.AssertNoErr(utils.WriteAll(fd, cmdBuf))
		}

		readResponse := func(fd *os.File) *quantumfs.PathsAccessed {
			fd.Seek(0, 0)
			size := quantumfs.BufferSize
			buf := make([]byte, quantumfs.BufferSize)
			result := make([]byte, 0)
			for size == quantumfs.BufferSize {
				size, err := fd.Read(buf)
				if err == io.EOF {
					break
				}
				test.AssertNoErr(err)
				test.Assert(size > 0, "read zero bytes")
				result = append(result, buf[:size]...)
			}
			result = bytes.TrimRight(result, "\u0000")

			var accesslistResponse quantumfs.AccessListResponse
			test.AssertNoErr(json.Unmarshal(result, &accesslistResponse))
			errorResponse := accesslistResponse.ErrorResponse
			test.Assert(errorResponse.ErrorCode == quantumfs.ErrorOK,
				"qfs command Error:%s", errorResponse.Message)
			return &accesslistResponse.PathList
		}

		verifyResponse := func(responselist *quantumfs.PathsAccessed,
			accessList quantumfs.PathsAccessed, expectedSize int) {

			test.Assert(mapKeySizeSum(responselist) == expectedSize,
				"Error getting unequal sizes %d != %d",
				mapKeySizeSum(responselist), expectedSize)
			test.assertAccessList(accessList, responselist,
				"Error two maps different")
		}

		writeRequest(workspace, fd1)
		writeRequest(workspace2, fd2)

		verifyResponse(readResponse(fd1), accessList, expectedSize)
		verifyResponse(readResponse(fd2), accessList2, expectedSize2)
	})
}

func TestApiClearAccessList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		accessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		api := test.getApi()

		relpath := test.RelPath(workspace)
		err = api.ClearAccessed(relpath)
		test.Assert(err == nil,
			"Error clearing accessList with api")
		accessList = quantumfs.NewPathsAccessed()
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error maps not clear")
	})
}

func getExtendedKeyHelper(test *testHelper, dst string, type_ string) string {
	key := make([]byte, quantumfs.ExtendedKeyLength)
	sz, err := syscall.Getxattr(dst, quantumfs.XAttrTypeKey, key)
	test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
		"Error getting the key of %s: %v with a size of %d", type_, err, sz)
	return string(key)
}

func ApiInsertInodeTest(test *testHelper, uid uint32, gid uint32) {
	api := test.getApi()

	// Create the source and the target workspace
	workspaceSrc := test.NewWorkspace()
	workspaceDst := test.NewWorkspace()
	dst := test.RelPath(workspaceDst)

	dirName := workspaceSrc + "/test/a"
	dirName1 := dirName + "/b"
	testFilename := dirName1 + "/test"
	linkFilename := workspaceSrc + "/link"
	spFilename := workspaceSrc + "/pipe"

	var PermissionA, PermissionB, expectedMode uint32
	PermissionA = syscall.S_IXUSR | syscall.S_IWGRP | syscall.S_IROTH
	PermissionB = syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO

	err := utils.MkdirAll(dirName1, os.FileMode(PermissionA))
	test.Assert(err == nil, "Error creating directories: %v", err)

	fd, err := syscall.Creat(testFilename, PermissionB)
	test.Assert(err == nil, "Error creating a small file: %v", err)
	syscall.Close(fd)

	err = syscall.Symlink(testFilename, linkFilename)
	test.Assert(err == nil, "Error creating a symlink: %v", err)

	expectedMode = syscall.S_IFIFO | syscall.S_IRWXU
	err = syscall.Mknod(spFilename, expectedMode,
		0x12345678)
	test.Assert(err == nil, "Error creating pipe")

	// get the key from a file
	keyF := getExtendedKeyHelper(test, testFilename, "file")

	// get the key from a directory
	keyD := getExtendedKeyHelper(test, dirName1, "directory")

	// get the key from a Symlink
	keyS := make([]byte, quantumfs.ExtendedKeyLength)
	sz, err, keyS := utils.LGetXattr(linkFilename, quantumfs.XAttrTypeKey,
		quantumfs.ExtendedKeyLength)
	test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
		"Error getting the Key of symlink: %v with a size of %d",
		err, sz)

	// get the key from a pipe
	keyP := getExtendedKeyHelper(test, spFilename, "pipe")

	dirNameD := test.AbsPath(dst + "/test/a")
	err = utils.MkdirAll(dirNameD, os.FileMode(PermissionA))
	test.Assert(err == nil, "Error creating target directories: %v", err)

	// Ensure the workspace root cannot be duplicated
	err = api.InsertInode(dst, keyF, PermissionA, uid, gid)
	test.Assert(err != nil,
		"Unexpected success duplicating workspace root")

	// Ensure the non-existing intermediate Inode not be created
	err = api.InsertInode(dst+"/nonExist/b", keyF, PermissionA, uid, gid)
	test.Assert(err != nil,
		"Unexpected success creating non-existing intermediate"+
			" Inode")

	// Stat the file before creating it to ensure that the negative entry is in
	// the kernel's cache. Then after we insert the file if we haven't properly
	// notified the kernel of the new entry the second stat will fail the test.
	var stat syscall.Stat_t
	err = syscall.Stat(workspaceDst+"/test/a/file", &stat)
	test.AssertErr(err)

	// Duplicate the file in the given path
	err = api.InsertInode(dst+"/test/a/file", keyF, PermissionA, uid, gid)
	test.Assert(err == nil,
		"Error duplicating a file to target workspace: %v", err)

	test.WaitFor("Inserted file to exist", func() bool {
		err = syscall.Stat(workspaceDst+"/test/a/file", &stat)
		return err == nil
	})

	// InsertInode with an empty key should fail
	err = api.InsertInode(dst+"/test/a/file1", "", PermissionA, uid, gid)
	test.Assert(err != nil, "Unexpected success inserting empty key")

	// check the mode of file
	expectedMode = syscall.S_IFREG | PermissionA
	test.Assert(stat.Mode == expectedMode,
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

	test.Assert(stat.Uid == expectedUid, "uid doesn't match %d %d",
		stat.Uid, expectedUid)
	test.Assert(stat.Gid == expectedGid, "gid doesn't match %d %d",
		stat.Gid, expectedGid)

	// Duplicate the directory in the given path
	err = api.InsertInode(dst+"/test/a/dirtest", keyD, PermissionA, uid, gid)
	test.Assert(err != nil,
		"Succeeded duplicating a directory to target workspace:")

	// Ensure the symlink in the given path
	err = api.InsertInode(dst+"/symlink", string(keyS), PermissionB, uid, gid)
	test.Assert(err == nil,
		"Error duplicating a symlink to workspace: %v", err)

	err = syscall.Lstat(workspaceDst+"/symlink", &stat)
	test.Assert(err == nil,
		"Error getting status of Symlink: %v", err)

	expectedMode = syscall.S_IFLNK | PermissionB
	test.Assert(stat.Mode == expectedMode,
		"Symlink mode incorrect. Expected %x got %x %d",
		expectedMode, stat.Mode, stat.Size)

	// Ensure the pipe file in the given path
	err = api.InsertInode(dst+"/Pipe", keyP, PermissionB, uid, gid)
	test.Assert(err == nil,
		"Error duplicating a pipe file to workspace: %v", err)

	err = syscall.Stat(workspaceDst+"/Pipe", &stat)
	test.Assert(err == nil,
		"Error getting status of Pipe file: %v", err)

	expectedMode = syscall.S_IFIFO | PermissionB
	test.Assert(stat.Mode == expectedMode,
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
		api, err := os.OpenFile(test.AbsPath(quantumfs.ApiPath),
			syscall.O_DIRECT, 0)
		test.Assert(err == nil, "Error opening api file: %v", err)
		defer api.Close()

		buf := make([]byte, 0, 256)
		n, err := api.Read(buf)
		test.Assert(n == 0, "Wrong number of bytes read: %d", n)
	})
}

func TestApiNoRequestNonBlockingRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api, err := os.OpenFile(test.AbsPath(quantumfs.ApiPath),
			syscall.O_DIRECT|syscall.O_NONBLOCK, 0)
		test.Assert(err == nil, "Error opening api file: %v", err)
		defer api.Close()

		// The file has set O_DIRECT flag, so the kernel won't trigger
		// Read() if the client buffer is zero
		buf := make([]byte, 0, 256)
		n, err := api.Read(buf)
		test.Assert(n == 0, "Wrong number of bytes read: %d", n)
		test.Assert(err == nil,
			"Non-blocking read api without requests error:%v", err)

		// Give the client buffer space to read from QuantumFs server
		buf = make([]byte, 256)
		api.Write(buf)
		n, err = api.Read(buf)
		test.Assert(n == 0, "Wrong number of bytes read: %d", n)
	})
}

func TestWorkspaceDeletion(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		api := test.getApi()
		ws1 := test.NewWorkspace()
		ws2 := test.NewWorkspace()

		test.AssertNoErr(api.DeleteWorkspace(test.RelPath(ws1)))

		test.assertNoFile(ws1)
		test.assertFileExists(ws2)
	})
}

func TestWorkspaceDeletionSameNamespaceInstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()
		ws0 := test.NewWorkspace()
		wsName0 := test.RelPath(ws0)
		parts := strings.Split(wsName0, "/")
		wsName1 := parts[0] + "/" + parts[1] + "/_branched"
		test.AssertNoErr(api.Branch(wsName0, wsName1))
		ws1 := test.AbsPath(wsName1)
		test.assertFileExists(ws1)

		_, cleanup := test.GetWorkspaceRoot(wsName1)
		// The workspace is instantiated, so handleDeletedWorkspace
		// will be called
		defer cleanup()

		test.AssertNoErr(api.DeleteWorkspace(test.RelPath(ws1)))
		test.assertFileExists(ws0)
		test.WaitForLogString("Out-- Mux::handleDeletedWorkspace",
			"handleDeletedWorkspace to finish")
		test.assertNoFile(ws1)
	})
}

func TestWorkspaceDeletionSameNamespaceUninstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()
		ws0 := test.NewWorkspace()
		wsName0 := test.RelPath(ws0)
		parts := strings.Split(wsName0, "/")
		wsName1 := parts[0] + "/" + parts[1] + "/_branched"
		test.AssertNoErr(api.Branch(wsName0, wsName1))
		ws1 := test.AbsPath(wsName1)
		test.assertFileExists(ws1)
		test.remountFilesystem()
		api = test.getApi()
		test.AssertNoErr(api.DeleteWorkspace(test.RelPath(ws1)))
		test.assertFileExists(ws0)
		test.assertNoFile(ws1)
	})
}

func TestApiGetAndSetBlock(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		key := []byte("11112222333344445555")
		data := GenData(300)
		err := api.SetBlock(key, data)
		test.AssertNoErr(err)

		readData, err := api.GetBlock(key)
		test.AssertNoErr(err)

		test.Assert(bytes.Equal(data, readData), "Data mismatch")

		// Ensure that we are checking the key length correctly
		err = api.SetBlock(key[:1], data)
		test.Assert(err != nil, "Invalid key length allowed in SetBlock")

		_, err = api.GetBlock(key[:1])
		test.Assert(err != nil, "Invalid key length allowed in GetBlock")
	})
}

func TestInvalidWorkspaceName(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		assertInvalid := func(err error) {
			test.AssertErr(err)
			test.Assert(strings.Contains(err.Error(), "workspace name"),
				"Expected 'workspace name' in error: %s",
				err.Error())
			test.Assert(strings.Contains(err.Error(), "is malformed"),
				"Expected 'is malformed' in error: %s",
				err.Error())
		}

		assertInvalid(api.DeleteWorkspace("//qfs"))
		assertInvalid(api.DeleteWorkspace("too/many/slashes/"))
	})
}
