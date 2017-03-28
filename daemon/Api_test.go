// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various Api calls

import "fmt"
import "os"
import "runtime"
import "syscall"
import "sync"
import "sync/atomic"
import "testing"
import "time"

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

func generateFiles(test *testHelper, size int, workspace,
	filename string) (map[string]bool, int) {

	accessList := make(map[string]bool)
	expectedSize := 0
	for i := 0; i < size; i++ {
		filename := fmt.Sprintf("/%s%d", filename, i)
		expectedSize += len(filename)
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
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

func mapKeySizeSum(list map[string]bool) int {
	size := 0
	for key, _ := range list {
		size += len(key)
	}

	return size
}

func TestApiAccessListEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		api := test.getApi()
		relpath := test.relPath(workspace)

		responselist, err := api.GetAccessed(relpath)
		test.assert(err == nil, "Error getting accessList with api")

		expectedSize := 0
		test.assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		accessList := make(map[string]bool)
		test.assertAccessList(accessList, responselist,
			"Error two maps different")
	})
}

func TestApiAccessListLargeSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		filename := "testfiletestfiletestfiletestfile" +
			"testfiletestfiletestfiletestfiletestfiletestfile"

		workspace := test.newWorkspace()
		accessList, expectedSize := generateFiles(test, 200,
			workspace, filename)

		api := test.getApi()
		relpath := test.relPath(workspace)

		responselist, err := api.GetAccessed(relpath)
		test.assert(err == nil, "Error getting accessList with api")

		test.assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		test.assertAccessList(accessList, responselist,
			"Error two maps different")
	})
}

func TestApiAccessListApiFileSizeResidue(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		filename := "testfiletestfiletestfiletestfiletestfiletesti" +
			"filetestfiletestfiletestfiletestfile"

		accessList, expectedSize := generateFiles(test,
			200, workspace, filename)

		api := test.getApi()
		relpath := test.relPath(workspace)

		responselist, _ := api.GetAccessed(relpath)
		queueSize1 := atomic.LoadInt64(&test.qfs.apiFileSize)
		test.assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		test.assertAccessList(accessList, responselist,
			"Error two maps different")

		test.qfs.setFileHandle(&test.qfs.c, 7, nil)
		queueSize2 := atomic.LoadInt64(&test.qfs.apiFileSize)
		test.assert(queueSize1 >= int64(expectedSize) && queueSize2 == 0,
			"The apiFileSize: %d %d, the actual response size: %d)",
			queueSize1, queueSize2, expectedSize)
	})
}

func TestApiAccessListConcurrent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		size := 100
		filename := "samplesamplesamplesamplesample" +
			"samplesamplesamplesamplesample"
		workspace := test.newWorkspace()
		accessList, expectedSize := generateFiles(test,
			size, workspace, filename)

		api := test.getApi()
		relpath := test.relPath(workspace)

		var wg sync.WaitGroup
		initFileSize, endFileSize, initFileSize2, endFileSize2 := 0, 0, 0, 0

		wg.Add(1)
		startApi := make(chan struct{})
		startApi2 := make(chan struct{})

		workspace2 := test.newWorkspace()

		filename2 := "concurrentconcurrentconcurrent" +
			"concurrentconcurrent"
		generateFiles(test, size, workspace2, filename2)

		relpath2 := test.relPath(workspace2)
		api2 := test.getUniqueApi(workspace2 + "/api")
		defer api2.Close()
		test.assert(api != api2, "Error getting the same file descriptor")

		go func() {
			defer wg.Done()
			close(startApi)
			<-startApi2
			initFileSize2 = int(
				atomic.LoadInt64(&test.qfs.apiFileSize))
			_, err := api2.GetAccessed(relpath2)
			test.assert(err == nil, "Error getting accessList with api2")
			endFileSize2 = int(
				atomic.LoadInt64(&test.qfs.apiFileSize))
		}()
		<-startApi
		close(startApi2)

		initFileSize = int(atomic.LoadInt64(&test.qfs.apiFileSize))
		responselist, err := api.GetAccessed(relpath)
		endFileSize = int(atomic.LoadInt64(&test.qfs.apiFileSize))

		wg.Wait()

		test.assert(err == nil, "Error getting accessList with api")

		test.assert(mapKeySizeSum(responselist) == expectedSize,
			"Error getting unequal sizes %d != %d",
			mapKeySizeSum(responselist), expectedSize)

		test.assertAccessList(accessList, responselist,
			"Error two maps different")

		// In order to prove two api goroutines ran concurrently. Two
		// conditions have to be satisfied. Only when two api goroutines
		// starts at the same point, they will share the same prior
		// apiFileSize. Only if their partial reads interleave, will the
		// posterior apiFileSizes be the same
		test.assert(initFileSize == initFileSize2 &&
			endFileSize == endFileSize2 && endFileSize != 0,
			"Error two api's aren't running in concurrent %d %d %d %d",
			initFileSize, initFileSize2, endFileSize, endFileSize2)
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

func getExtendedKeyHelper(test *testHelper, dst string, type_ string) string {
	key := make([]byte, quantumfs.ExtendedKeyLength)
	sz, err := syscall.Getxattr(dst, quantumfs.XAttrTypeKey, key)
	test.assert(err == nil && sz == quantumfs.ExtendedKeyLength,
		"Error getting the key of %s: %v with a size of %d", type_, err, sz)
	return string(key)
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
	err = api.InsertInode(dst, keyF, PermissionA, uid, gid)
	test.assert(err != nil,
		"Unexpected success duplicating workspace root")

	// Ensure the non-existing intermediate Inode not be created
	err = api.InsertInode(dst+"/nonExist/b", keyF, PermissionA, uid, gid)
	test.assert(err != nil,
		"Unexpected success creating non-existing intermediate"+
			" Inode")

	// Duplicate the file in the given path
	err = api.InsertInode(dst+"/test/a/file", keyF, PermissionA, uid, gid)
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
	err = api.InsertInode(dst+"/test/a/dirtest", keyD, PermissionA, uid, gid)
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

	// check the child is a file
	expectedMode = syscall.S_IFREG | PermissionB
	test.assert(stat.Mode == expectedMode,
		"Directory's file mode incorrect. Expected %x got %x",
		expectedMode, stat.Mode)

	// Ensure the symlink in the given path
	err = api.InsertInode(dst+"/symlink", string(keyS), PermissionB, uid, gid)
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
	err = api.InsertInode(dst+"/Pipe", keyP, PermissionB, uid, gid)
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

func TestApiInsertOverExisting(t *testing.T) {
	configModifier := func(test *testHelper, config *QuantumFsConfig) {
		cacheTimeout100Ms(test, config)
		dirtyDelay100Ms(test, config)
	}

	runTestCustomConfig(t, configModifier, func(test *testHelper) {
		testApiInsertOverExisting(test, nil, nil)
	})
}

func TestApiInsertOverExistingOpenInodes(t *testing.T) {
	configModifier := func(test *testHelper, config *QuantumFsConfig) {
		cacheTimeout100Ms(test, config)
		dirtyDelay100Ms(test, config)
	}

	runTestCustomConfig(t, configModifier, func(test *testHelper) {
		var dir2 *os.File
		var file2 int

		defer func() {
			syscall.Close(file2)
			dir2.Close()
		}()

		openInodes := func(workspace string) {
			var err error
			dir2, err = os.Open(workspace + "/dir1/dir2")
			test.assertNoErr(err)
			_, err = dir2.Readdirnames(2)
			test.assertNoErr(err)

			file2, err = syscall.Open(workspace+"/dir1/dir2/file2",
				os.O_RDWR, 0)
			test.assertNoErr(err)
		}

		checkInodes := func(workspace string) {
			_, err := dir2.Seek(0, os.SEEK_SET)
			test.assertNoErr(err)
			_, err = dir2.Readdirnames(-1)
			test.assertNoErr(err)

			test.log("Reading from file")
			buf := make([]byte, 100, 100)
			n, err := syscall.Read(file2, buf)
			test.assertNoErr(err)
			test.assert(n > 0, "Read no bytes from deleted file: %d", n)

			test.log("Writing to file")
			n, err = syscall.Write(file2, []byte("arstarstarst"))
			test.assertNoErr(err)
			test.assert(n > 0, "Wrote no bytes to deleted file: %d", n)
		}

		testApiInsertOverExisting(test, openInodes, checkInodes)
	})
}

func TestApiInsertOverExistingForget(t *testing.T) {
	configModifier := func(test *testHelper, config *QuantumFsConfig) {
		cacheTimeout100Ms(test, config)
		dirtyDelay100Ms(test, config)
	}

	runTestCustomConfig(t, configModifier, func(test *testHelper) {
		messages := make([]TLA, 0, 10)

		findInodes := func(workspace string) {
			for _, file := range []string{"/dir1/file1",
				"/dir1/dir2/file2", "/dir1/dir2/dir3/file3"} {

				inodeNum := test.getInodeNum(workspace + file)
				msg := fmt.Sprintf("Forget called on inode %d",
					inodeNum)
				tla := TLA{
					mustContain: true,
					text:        msg,
					failMsg:     "Subtree inode not forgotten",
				}
				messages = append(messages, tla)
			}
		}

		triggerForget := func(workspace string) {
			test.remountFilesystem()
		}

		testApiInsertOverExisting(test, findInodes, triggerForget)

		test.assertTestLog(messages)
	})
}

func testApiInsertOverExisting(test *testHelper, tamper1 func(workspace string),
	tamper2 func(workspace string)) {

	srcWorkspace := test.newWorkspace()
	dir1 := srcWorkspace + "/dir1"
	dir2 := dir1 + "/dir2"
	dir3 := dir2 + "/dir3"

	err := os.MkdirAll(srcWorkspace+"/dir1/dir2/dir3", 0777)
	test.assertNoErr(err)

	err = printToFile(dir1+"/file1", "")
	test.assertNoErr(err)
	err = printToFile(dir2+"/file2", "oienoenoienoin")
	test.assertNoErr(err)
	err = printToFile(dir3+"/file3", "")
	test.assertNoErr(err)

	dstWorkspace := test.absPath(test.branchWorkspace(srcWorkspace))

	// Create one marker file in srcWorkspace and dstWorkspace
	err = printToFile(dir1+"/srcMarker", "")
	test.assertNoErr(err)
	err = printToFile(dstWorkspace+"/dir1/dstMarker", "")
	test.assertNoErr(err)

	if tamper1 != nil {
		tamper1(dstWorkspace)
	}

	dir1Key := getExtendedKeyHelper(test, dir1, "dir1 key")

	api := test.getApi()

	err = api.InsertInode(test.relPath(dstWorkspace)+"/dir1", dir1Key,
		0777, 0, 0)
	test.assertNoErr(err)

	time.Sleep(200 * time.Millisecond)

	// Now dir1/dstMarker should not exist and dir1/srcMarker should
	file, err := os.Open(dstWorkspace + "/dir1/dstMarker")
	test.assert(err != nil, "dstMarker still exists!")

	file, err = os.Open(srcWorkspace + "/dir1/srcMarker")
	test.assertNoErr(err)
	defer file.Close()

	if tamper2 != nil {
		tamper2(dstWorkspace)
	}
}

func TestApiNoRequestBlockingRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api, err := os.OpenFile(test.absPath(quantumfs.ApiPath),
			syscall.O_DIRECT, 0)
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
			syscall.O_DIRECT|syscall.O_NONBLOCK, 0)
		test.assert(err == nil, "Error opening api file: %v", err)
		defer api.Close()

		// The file has set O_DIRECT flag, so the kernel won't trigger
		// Read() if the client buffer is zero
		buf := make([]byte, 0, 256)
		n, err := api.Read(buf)
		test.assert(n == 0, "Wrong number of bytes read: %d", n)
		test.assert(err == nil,
			"Non-blocking read api without requests error:%v", err)

		// Give the client buffer space to read from QuantumFs server
		buf = make([]byte, 256)
		api.Write(buf)
		n, err = api.Read(buf)
		test.assert(n == 0, "Wrong number of bytes read: %d", n)
		if runtime.Version() != "go1.7.3" {
			test.assert(err.(*os.PathError).Err == syscall.EAGAIN,
				"Non-blocking read api without requests error:%v",
				err)
		}
	})
}

func TestWorkspaceDeletion(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		api := test.getApi()

		ws1 := test.newWorkspace()
		ws2 := test.newWorkspace()

		err := api.DeleteWorkspace(test.relPath(ws1))
		test.assert(err == nil, "Failed to delete workspace: %v", err)

		time.Sleep(200 * time.Millisecond)

		var stat syscall.Stat_t
		err = syscall.Stat(ws1, &stat)
		test.assert(err != nil, "Workspace1 not deleted")

		err = syscall.Stat(ws2, &stat)
		test.assert(err == nil, "Workspace2 deleted: %v", err)
	})
}
