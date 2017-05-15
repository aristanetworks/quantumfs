// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "io/ioutil"
import "testing"
import "syscall"
import "os"
import "fmt"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/testutils"

func getRootId(test *testHelper, workspace string) quantumfs.ObjectKey {
	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	return test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)
}

func advanceWorkspace(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	wsdb := test.GetWorkspaceDB()

	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
		wsNamespaceName, wsWorkspaceName, src, dst)
	test.AssertNoErr(err)
}

func createTestFileNoSync(test *testHelper,
	workspace string, name string, size int) {

	filename := workspace + "/" + name
	err := testutils.PrintToFile(filename, string(GenData(size)))
	test.AssertNoErr(err)
}

func createTestFile(c *ctx, test *testHelper,
	workspace string, name string, size int) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)
	createTestFileNoSync(test, workspace, name, size)
	test.SyncAllWorkspaces()
	newRootId := getRootId(test, workspace)
	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")
	c.vlog("Created file %s and new rootID is %s", name, newRootId.String())

	return newRootId
}

func removeTestFileNoSync(test *testHelper,
	workspace string, name string) {

	filename := workspace + "/" + name
	err := os.Remove(filename)
	test.AssertNoErr(err)
}

func removeTestFile(c *ctx, test *testHelper,
	workspace string, name string) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)
	removeTestFileNoSync(test, workspace, name)
	test.SyncAllWorkspaces()
	newRootId := getRootId(test, workspace)
	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")
	c.vlog("Removed file %s and new rootID is %s", name, newRootId.String())

	return newRootId
}

func linkTestFileNoSync(c *ctx, test *testHelper,
	workspace string, src string, dst string) {

	srcfilename := workspace + "/" + src
	dstfilename := workspace + "/" + dst
	c.vlog("Before link %s -> %s", src, dst)
	err := syscall.Link(srcfilename, dstfilename)
	test.AssertNoErr(err)

}

func linkTestFile(c *ctx, test *testHelper,
	workspace string, src string, dst string) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)
	linkTestFileNoSync(c, test, workspace, src, dst)
	test.SyncAllWorkspaces()
	newRootId := getRootId(test, workspace)
	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")
	c.vlog("Created link %s -> %s and new rootID is %s", src, dst,
		newRootId.String())

	return newRootId
}

func markImmutable(ctx *ctx, workspace string) {
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	ctx.qfs.workspaceMutability[workspace] = workspaceImmutable
}

func markMutable(ctx *ctx, workspace string) {
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	ctx.qfs.workspaceMutability[workspace] = workspaceMutable
}

func refreshTo(c *ctx, test *testHelper, workspace string, dst quantumfs.ObjectKey) {
	wsr, cleanup := test.getWorkspaceRoot(workspace)
	defer cleanup()
	test.Assert(wsr != nil, "workspace root does not exist")
	wsr.refresh(c, dst)
}

func refreshTestNoRemount(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	markImmutable(ctx, workspace)
	advanceWorkspace(ctx, test, workspace, src, dst)
	refreshTo(ctx, test, workspace, dst)
	markMutable(ctx, workspace)
}

func refreshTest(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	markImmutable(ctx, workspace)
	test.remountFilesystem()
	advanceWorkspace(ctx, test, workspace, src, dst)
	refreshTo(ctx, test, workspace, dst)
	markMutable(ctx, workspace)
}

func TestRefreshFileAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
	})
}

func TestRefreshUnchanged(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)

		markImmutable(ctx, workspace)
		test.remountFilesystem()
		refreshTo(ctx, test, workspace, newRootId1)
		markMutable(ctx, workspace)

		newRootId2 := getRootId(test, workspace)
		test.Assert(newRootId2.IsEqualTo(newRootId1),
			"Refresh to current rootId must be a noop")
	})
}

func TestRefreshFileRewrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"

		ctx := test.TestCtx()
		createTestFile(ctx, test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := createTestFile(ctx, test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := getRootId(test, workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		removeTestFile(ctx, test, workspace, name)
	})
}

func TestRefreshFileRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"

		ctx := test.TestCtx()
		createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		newRootId1 := removeTestFile(ctx, test, workspace, name)
		newRootId2 := createTestFile(ctx, test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		filename := workspace + "/" + name
		err := os.Remove(filename)
		test.Assert(err != nil, "The file must not exist after refresh")
	})
}

func TestRefreshHardlinkAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()
		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		newRootId1 := linkTestFile(ctx, test, workspace, name, linkfile)
		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
		newRootId3 := removeTestFile(ctx, test, workspace, linkfile)

		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshHardlinkRemoval(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := linkTestFile(ctx, test, workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
		linkname := workspace + "/" + linkfile
		err := os.Remove(linkname)
		test.Assert(err != nil, "The linkfile must not exist after refresh")
	})
}

func TestRefreshNlinkDrop(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		var newRootId2 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId2 = linkTestFile(ctx, test, workspace, name,
				linkfile)
		}

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(ctx, test, workspace, name)
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlinkBump(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		var newRootId1 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId1 = linkTestFile(ctx, test, workspace, name,
				linkfile)
		}

		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(ctx, test, workspace, name)

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId3 = removeTestFile(ctx, test, workspace, linkfile)
		}
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func assertFileIsOfSize(test *testHelper, fullname string, size int64) {
	var stat syscall.Stat_t

	err := syscall.Stat(fullname, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Size == size,
		"Incorrect file size. Expected: %d", stat.Size)
}

func assertOpenFileIsOfSize(test *testHelper, fd int, size int64) {
	var stat syscall.Stat_t

	err := syscall.Fstat(fd, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Size == size,
		"Incorrect file size. Expected: %d", stat.Size)
}

func TestRefreshOpenFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		fullname := workspace + "/" + name

		ctx := test.TestCtx()
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := createTestFile(ctx, test, workspace, name, 2000)

		file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		assertFileIsOfSize(test, fullname, 3000)
		assertOpenFileIsOfSize(test, int(file.Fd()), 3000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := getRootId(test, workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		assertOpenFileIsOfSize(test, int(file.Fd()), 1000)
		assertFileIsOfSize(test, fullname, 1000)

		err = file.Close()
		test.AssertNoErr(err)

		removeTestFile(ctx, test, workspace, name)
	})
}

func TestRefreshUninstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		const (
			nfiles = 30
			ndirs  = 10
			pardir = "/pardir"
		)
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+pardir, 0777)
		for i := 0; i < ndirs; i++ {
			name := fmt.Sprintf("%s/%s/%d", workspace, pardir, i)
			utils.MkdirAll(name, 0777)
		}

		ctx := test.TestCtx()

		for i := 0; i < nfiles; i++ {
			name := fmt.Sprintf("%s/%d/%d", pardir, i%ndirs, i)
			createTestFileNoSync(test, workspace, name, 1000)
		}
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		for i := 0; i < nfiles; i++ {
			name := fmt.Sprintf("%s/%d/%d", pardir, i%ndirs, i)
			removeTestFileNoSync(test, workspace, name)
		}
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		test.AssertLogContains("Adding uninstantiated",
			"There are no uninstantiated inodes")

		for i := 0; i < nfiles; i++ {
			name := fmt.Sprintf("%s/%d/%d", pardir, i%ndirs, i)
			removeTestFile(ctx, test, workspace, name)
		}
	})
}

func TestRefreshChangeTypeDirToHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, name, 1000)
		newRootId1 := linkTestFile(ctx, test, workspace, name, linkfile)
		removeTestFileNoSync(test, workspace, name)
		utils.MkdirAll(workspace+"/"+name, 0777)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
		removeTestFile(ctx, test, workspace, linkfile)
	})
}

func TestRefreshCachedDeletedEntry(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		fulldirame := workspace + "/subdir"
		filename := "testfile"
		fullfilename := workspace + "/" + filename

		utils.MkdirAll(fulldirame, 0777)
		_, err := os.Stat(fulldirame)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()
		newRootId1 := createTestFile(ctx, test, workspace, filename, 1000)
		_, err = os.Stat(fullfilename)
		test.AssertNoErr(err)

		err = os.RemoveAll(fulldirame)
		test.AssertNoErr(err)

		err = os.RemoveAll(fullfilename)
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		_, err = os.Stat(fulldirame)
		test.Assert(err != nil, "stat succeeded after delete")
		_, err = os.Stat(fullfilename)
		test.Assert(err != nil, "stat succeeded after delete")

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		_, err = os.Stat(fulldirame)
		test.AssertNoErr(err)
		_, err = os.Stat(fullfilename)
		test.AssertNoErr(err)
	})
}

func TestRefreshChangeTypeDirToFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)

		removeTestFileNoSync(test, workspace, name)
		utils.MkdirAll(workspace+"/"+name, 0777)
		utils.MkdirAll(workspace+"/"+name+"/subdir", 0777)
		createTestFileNoSync(test, workspace, name+"/subfile", 1000)
		createTestFileNoSync(test, workspace, name+"/subdir/subfile", 1000)
		utils.MkdirAll(workspace+"/"+name+"/subdir/subdir1", 0777)

		subfile1name := workspace + "/" + name + "/subfile"
		subfile1, err := os.OpenFile(subfile1name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		subfile2name := workspace + "/" + name + "/subdir/subfile"
		subfile2, err := os.OpenFile(subfile2name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		err = subfile1.Close()
		test.AssertNoErr(err)

		err = subfile2.Close()
		test.AssertNoErr(err)

		removeTestFile(ctx, test, workspace, name)

		_, err = os.OpenFile(subfile1name, os.O_RDONLY, 0777)
		test.Assert(err != nil, " err is not nil")

		_, err = os.OpenFile(subfile2name, os.O_RDONLY, 0777)
		test.Assert(err != nil, " err is not nil")

		_, err = os.Stat(workspace + "/" + name + "/subdir/subdir1")
		test.Assert(err != nil, "stat succeeded after refresh")
	})
}

func TestRefreshChangeTypeFileToDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		utils.MkdirAll(workspace+"/"+name, 0777)
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)
		removeTestFileNoSync(test, workspace, name)
		newRootId2 := createTestFile(ctx, test, workspace, name, 1000)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		err := syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)
	})
}

func TestRefreshOpenFileContentCheck(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		fullname := workspace + "/" + name
		content1 := "The original content"

		ctx := test.TestCtx()
		err := testutils.PrintToFile(fullname, content1)
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		err = testutils.OverWriteFile(fullname, "Not the original content")
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		content2, err := ioutil.ReadFile(fullname)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := getRootId(test, workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		err = file.Close()
		test.AssertNoErr(err)

		content3, err := ioutil.ReadFile(fullname)
		test.AssertNoErr(err)

		ctx.vlog("content1 %s, content2 %s, content3 %s",
			string(content1), string(content2), string(content3))
		test.Assert(string(content1) == string(content3), "content mismatch")

		removeTestFile(ctx, test, workspace, name)
	})
}
