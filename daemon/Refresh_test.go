// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

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

	wsr, cleanup := test.getWorkspaceRoot(workspace)
	defer cleanup()
	test.Assert(wsr != nil, "workspace root does not exist")
	wsr.publishedRootId = dst
}

func synced_op(test *testHelper, workspace string,
	nosync_op func()) quantumfs.ObjectKey {

	test.SyncAllWorkspaces()
	oldRootId := getRootId(test, workspace)
	nosync_op()
	test.SyncAllWorkspaces()
	newRootId := getRootId(test, workspace)
	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")
	test.Log("new rootID %s", newRootId.String())

	return newRootId
}

func createTestFileNoSync(test *testHelper,
	workspace string, name string, size int) {

	filename := workspace + "/" + name
	err := testutils.PrintToFile(filename, string(GenData(size)))
	test.AssertNoErr(err)
}

func createTestFile(test *testHelper,
	workspace string, name string, size int) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		createTestFileNoSync(test, workspace, name, size)
	})
}

func removeTestFileNoSync(test *testHelper,
	workspace string, name string) {

	filename := workspace + "/" + name
	err := os.Remove(filename)
	test.AssertNoErr(err)
}

func removeTestFile(test *testHelper,
	workspace string, name string) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		removeTestFileNoSync(test, workspace, name)
	})
}

func linkTestFileNoSync(test *testHelper,
	workspace string, src string, dst string) {

	srcfilename := workspace + "/" + src
	dstfilename := workspace + "/" + dst
	test.Log("Before link %s -> %s", src, dst)
	err := syscall.Link(srcfilename, dstfilename)
	test.AssertNoErr(err)

}

func linkTestFile(test *testHelper,
	workspace string, src string, dst string) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		linkTestFileNoSync(test, workspace, src, dst)
	})
}

func setXattrTestFileNoSync(test *testHelper, workspace string,
	testfile string, attr string, data []byte) {

	testFilename := workspace + "/" + testfile
	test.Log("Before setting xattr %s on %s", attr, testfile)
	err := syscall.Setxattr(testFilename, attr, data, 0)
	test.AssertNoErr(err)
}

func verifyXattr(test *testHelper, workspace string,
	testfile string, attr string, content []byte) {

	data := make([]byte, 100)
	size, err := syscall.Getxattr(workspace+"/"+testfile, attr, data)
	test.Assert(err == nil, "Error reading data XAttr: %v", err)
	test.Assert(size == len(content),
		"data XAttr size incorrect: %d", size)
	test.Assert(bytes.Equal(data[:size], content),
		"Didn't get the same data back '%s' '%s'", data,
		content)
}

func verifyNoXattr(test *testHelper, workspace string,
	testfile string, attr string) {

	data := make([]byte, 100)
	_, err := syscall.Getxattr(workspace+"/"+testfile, attr, data)
	test.AssertErr(err)
	test.Assert(err == syscall.ENODATA, "xattr must not exist %s", err.Error())
}

func setXattrTestFile(test *testHelper,
	workspace string, testfile string, attr string,
	data []byte) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		setXattrTestFileNoSync(test, workspace, testfile, attr, data)
	})
}

func delXattrTestFileNoSync(test *testHelper,
	workspace string, testfile string, attr string) {

	testFilename := workspace + "/" + testfile
	test.Log("Before removing xattr %s on %s", attr, testfile)
	err := syscall.Removexattr(testFilename, attr)
	test.AssertNoErr(err)
}

func delXattrTestFile(test *testHelper,
	workspace string, testfile string, attr string) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		delXattrTestFileNoSync(test, workspace, testfile, attr)
	})
}

func createTestSymlinkNoSync(test *testHelper, workspace string, name string,
	symname string) {

	err := syscall.Symlink(workspace+"/"+name, workspace+"/"+symname)
	test.AssertNoErr(err)
}

func createTestSymlink(test *testHelper,
	workspace string, name string, symname string) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		createTestSymlinkNoSync(test, workspace, name, symname)
	})
}

func createTestSpecialFileNoSync(test *testHelper, workspace string, name string,
	dev int) {

	err := syscall.Mknod(workspace+"/"+name, syscall.S_IFBLK|syscall.S_IRWXU,
		dev)
	test.AssertNoErr(err)
}

func createTestSpecialFile(test *testHelper,
	workspace string, name string, dev int) quantumfs.ObjectKey {

	return synced_op(test, workspace, func() {
		createTestSpecialFileNoSync(test, workspace, name, dev)
	})
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
	wsr.refreshTo(c, dst)
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

		newRootId1 := createTestFile(test, workspace, name, 1000)
		newRootId2 := removeTestFile(test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(test, workspace, name)
	})
}

func TestRefreshUnchanged(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()
		newRootId1 := createTestFile(test, workspace, name, 1000)

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
		createTestFile(test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(test, workspace, name, 1000)
		newRootId2 := createTestFile(test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := getRootId(test, workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		removeTestFile(test, workspace, name)
	})
}

func TestRefreshFileRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"

		ctx := test.TestCtx()
		createTestFile(test, workspace, "otherfile", 1000)
		createTestFile(test, workspace, name, 1000)
		newRootId1 := removeTestFile(test, workspace, name)
		newRootId2 := createTestFile(test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		filename := workspace + "/" + name
		err := os.Remove(filename)
		test.Assert(err != nil, "The file must not exist after refresh")
	})
}

func refreshHardlinkAdditionTestGen(rmLink0 bool, link1 bool,
	link2 bool) func(*testHelper) {

	return func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()
		oldRootId := createTestFile(test, workspace, "otherfile", 1000)
		createTestFile(test, workspace, name, 1000)

		content := "content to be verified"
		err := createSmallFile(workspace+"/"+name, content)
		test.AssertNoErr(err)

		if link1 {
			createTestFileNoSync(test, workspace, name+".link1", 1000)
			linkTestFileNoSync(test, workspace, name+".link1",
				linkfile+".link1")
		}
		newRootId1 := linkTestFile(test, workspace, name, linkfile)
		if rmLink0 {
			removeTestFileNoSync(test, workspace, linkfile)
		}
		if link2 {
			createTestFileNoSync(test, workspace, name+".link2", 1000)
			linkTestFileNoSync(test, workspace, name+".link2",
				linkfile+".link2")
		}

		newRootId2 := removeTestFile(test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		file, err := os.OpenFile(workspace+"/"+name, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file, content)
		err = file.Close()
		test.AssertNoErr(err)

		if link1 {
			removeTestFileNoSync(test, workspace, name+".link1")
			removeTestFileNoSync(test, workspace, linkfile+".link1")
		}

		_, err = os.Stat(workspace + "/" + linkfile + ".link2")
		test.AssertErr(err)

		removeTestFile(test, workspace, name)
		newRootId3 := removeTestFile(test, workspace, linkfile)

		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	}
}

func TestRefreshHardlinkAddition1(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(false, false, false))
}

func TestRefreshHardlinkAddition2(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(false, false, true))
}

func TestRefreshHardlinkAddition3(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(false, true, false))
}

func TestRefreshHardlinkAddition4(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(false, true, true))
}

func TestRefreshHardlinkAddition5(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(true, false, false))
}

func TestRefreshHardlinkAddition6(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(true, false, true))
}

func TestRefreshHardlinkAddition7(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(true, true, false))
}

func TestRefreshHardlinkAddition8(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(true, true, true))
}

func TestRefreshOrphanedHardlinkContentCheck(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		fullname := workspace + "/testFile"
		content := "original content"
		appendContent := " appended."

		ctx := test.TestCtx()

		newRootId1 := createTestFile(test, workspace, "otherfile", 1000)
		createHardlink(fullname, content)
		newRootId2 := getRootId(test, workspace)

		file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file, content)
		assertOpenFileIsOfSize(test, int(file.Fd()), int64(len(content)))

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		verifyContentStartsWith(test, file, content)
		assertNoFile(test, fullname)
		assertOpenFileIsOfSize(test, int(file.Fd()), int64(len(content)))
		_, err = file.Write([]byte(appendContent))
		test.AssertNoErr(err)
		assertNoFile(test, fullname)
		assertOpenFileIsOfSize(test, int(file.Fd()),
			int64(len(content)+len(appendContent)))
		verifyContentStartsWith(test, file, content+appendContent)

		err = file.Close()
		test.AssertNoErr(err)
	})
}

func TestRefreshHardlinkRemoval(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(test, workspace, name, 1000)
		newRootId2 := linkTestFile(test, workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(test, workspace, name)
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

		oldRootId := createTestFile(test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(test, workspace, name, 1000)
		var newRootId2 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId2 = linkTestFile(test, workspace, name,
				linkfile)
		}

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(test, workspace, name)
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlinkBump(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := createTestFile(test, workspace, "otherfile", 1000)
		createTestFile(test, workspace, name, 1000)
		var newRootId1 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId1 = linkTestFile(test, workspace, name,
				linkfile)
		}

		newRootId2 := removeTestFile(test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(test, workspace, name)

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId3 = removeTestFile(test, workspace, linkfile)
		}
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlink3To2(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		createTestFile(test, workspace, name, 1000)
		newRootId1 := linkTestFile(test, workspace, name, "link1")
		newRootId2 := linkTestFile(test, workspace, name, "link2")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		// Create and delete a temporary file to make sure a new rootId
		// is published
		createTestFileNoSync(test, workspace, name+".tmp", 1000)
		newRootId3 := removeTestFile(test, workspace, name+".tmp")

		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")
		removeTestFile(test, workspace, "link1")
		removeTestFile(test, workspace, name)
	})
}

func TestRefreshNlink2To3(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		createTestFile(test, workspace, name, 1000)
		linkTestFile(test, workspace, name, "link1")
		newRootId1 := linkTestFile(test, workspace, name, "link2")
		newRootId2 := removeTestFile(test, workspace, "link2")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		// Create and delete a temporary file to make sure a new rootId
		// is published
		createTestFileNoSync(test, workspace, name+".tmp", 1000)
		newRootId3 := removeTestFile(test, workspace, name+".tmp")

		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")
		removeTestFile(test, workspace, "link2")
		removeTestFile(test, workspace, "link1")
		removeTestFile(test, workspace, name)
	})
}

func assertFileIsOfSize(test *testHelper, fullname string, size int64) {
	var stat syscall.Stat_t

	err := syscall.Stat(fullname, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Size == size,
		"Incorrect file size. Expected: %d", stat.Size)
}

func assertNoFile(test *testHelper, fullname string) {
	var stat syscall.Stat_t
	err := syscall.Stat(fullname, &stat)
	test.AssertErr(err)
	test.Assert(err == syscall.ENOENT, "Expected ENOENT, got %s", err.Error())
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
		newRootId1 := createTestFile(test, workspace, name, 1000)
		newRootId2 := createTestFile(test, workspace, name, 2000)

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

		removeTestFile(test, workspace, name)
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
			removeTestFile(test, workspace, name)
		}
	})
}

func TestRefreshUninstantiatedInodeId(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		subdir := "subdir"
		filename := "testfile"
		fulldirame := workspace + "/" + subdir
		fullfilename := fulldirame + "/" + filename

		utils.MkdirAll(fulldirame, 0777)
		newRootId1 := createTestFile(test, workspace, subdir+"/"+filename, 0)
		// remount to make sure the corresponding inode is uninstantiated
		test.remountFilesystem()
		newRootId2 := createTestFile(test, workspace, subdir+"/otherfile", 0)

		var stat syscall.Stat_t
		err := syscall.Stat(fullfilename, &stat)
		test.AssertNoErr(err)
		inodeId := stat.Ino

		ctx := test.TestCtx()
		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		err = syscall.Stat(fullfilename, &stat)
		test.AssertNoErr(err)
		test.Assert(inodeId == stat.Ino, "inode mismatch %d vs %d",
			inodeId, stat.Ino)
	})
}

func TestRefreshChangeTypeDirToHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, name, 1000)
		newRootId1 := linkTestFile(test, workspace, name, linkfile)
		removeTestFileNoSync(test, workspace, name)
		utils.MkdirAll(workspace+"/"+name, 0777)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(test, workspace, name)
		removeTestFile(test, workspace, linkfile)
	})
}

func TestRefreshChangeTypeHardlinkToDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		utils.MkdirAll(workspace+"/"+name, 0777)
		utils.MkdirAll(workspace+"/"+linkfile, 0777)
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		err := syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)

		err = syscall.Rmdir(workspace + "/" + linkfile)
		test.AssertNoErr(err)
		createTestFileNoSync(test, workspace, name, 1000)
		newRootId2 := linkTestFile(test, workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		createTestFileNoSync(test, workspace, name+"/subfile", 1000)
		removeTestFileNoSync(test, workspace, name+"/subfile")
		err = syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)

		err = syscall.Rmdir(workspace + "/" + linkfile)
		test.AssertNoErr(err)
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
		newRootId1 := createTestFile(test, workspace, filename, 1000)
		_, err = os.Stat(fullfilename)
		test.AssertNoErr(err)

		err = os.RemoveAll(fulldirame)
		test.AssertNoErr(err)

		err = os.RemoveAll(fullfilename)
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		_, err = os.Stat(fulldirame)
		test.AssertErr(err)
		_, err = os.Stat(fullfilename)
		test.AssertErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		_, err = os.Stat(fulldirame)
		test.AssertNoErr(err)
		_, err = os.Stat(fullfilename)
		test.AssertNoErr(err)
	})
}

func TestRefreshDeleteWorkspaceRootFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, name, 1000)
		newRootId1 := removeTestFile(test, workspace, name)
		newRootId2 := createTestFile(test, workspace, name, 1000)

		f, err := os.OpenFile(workspace+"/"+name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		err = f.Close()
		test.AssertNoErr(err)

		_, err = os.OpenFile(workspace+"/"+name, os.O_RDONLY, 0777)
		test.AssertErr(err)
	})
}

func TestRefreshChangeTypeDirToFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(test, workspace, name, 1000)

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

		removeTestFile(test, workspace, name)

		_, err = os.OpenFile(subfile1name, os.O_RDONLY, 0777)
		test.AssertErr(err)

		_, err = os.OpenFile(subfile2name, os.O_RDONLY, 0777)
		test.AssertErr(err)

		_, err = os.Stat(workspace + "/" + name + "/subdir/subdir1")
		test.AssertErr(err)
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
		newRootId2 := createTestFile(test, workspace, name, 1000)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		err := syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)
	})
}

// Returns 1 if the content matches, 0 otherwise
func doesContentStartWith(test *testHelper, file *os.File, expected string) int {
	content := make([]byte, len(expected))
	_, err := file.Seek(0, os.SEEK_SET)
	test.AssertNoErr(err)
	_, err = io.ReadFull(file, content)
	test.AssertNoErr(err)
	if string(content) == expected {
		return 1
	} else {
		return 0
	}
}
func verifyContentStartsWith(test *testHelper, file *os.File, expected string) {
	test.Assert(doesContentStartWith(test, file, expected) == 1,
		fmt.Sprintf("content mismatch"))
}

type createFunc func(name string, content string) error

func contentTest(ctx *ctx, test *testHelper, c1 createFunc, c2 createFunc) {
	workspace := test.NewWorkspace()
	content1 := "original content"
	content2 := "CONTENT2"
	utils.MkdirAll(workspace+"/subdir", 0777)
	name := "subdir/testFile"
	fullname := workspace + "/" + name

	err := c1(fullname, content1)
	test.AssertNoErr(err)
	test.SyncAllWorkspaces()
	newRootId1 := getRootId(test, workspace)

	err = c2(fullname, content2)
	test.AssertNoErr(err)
	test.SyncAllWorkspaces()
	newRootId2 := getRootId(test, workspace)

	file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
	test.AssertNoErr(err)
	verifyContentStartsWith(test, file, content2)

	refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

	newRootId3 := getRootId(test, workspace)
	test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

	verifyContentStartsWith(test, file, content1)
	err = file.Close()
	test.AssertNoErr(err)

	file, err = os.Open(fullname)
	test.AssertNoErr(err)
	verifyContentStartsWith(test, file, content1)
	err = file.Close()
	test.AssertNoErr(err)
	removeTestFile(test, workspace, name)
}

func createSparseFile(name string, size int64) error {
	fd, err := syscall.Creat(name, 0124)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	return os.Truncate(name, size)
}

func createHardlink(name string, content string) error {
	fd, err := syscall.Creat(name, syscall.O_CREAT)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	err = testutils.OverWriteFile(name, content)
	if err != nil {
		return err
	}
	return syscall.Link(name, name+"_link")
}

func purgeHardlinkThenCreate(create createFunc) createFunc {
	return func(name string, content string) error {
		err := syscall.Unlink(name)
		if err != nil {
			return err
		}
		err = syscall.Unlink(name + "_link")
		if err != nil {
			return err
		}
		return create(name, content)
	}
}

func createSmallFile(name string, content string) error {
	fd, err := syscall.Creat(name, 0124)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func createMediumFile(name string, content string) error {
	size := int64(quantumfs.MaxMediumFileSize()) -
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func createLargeFile(name string, content string) error {
	size := int64(quantumfs.MaxMediumFileSize()) +
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func createVeryLargeFile(name string, content string) error {
	size := int64(quantumfs.MaxLargeFileSize()) +
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func contentCheckTestGen(c1 createFunc, c2 createFunc) func(*testHelper) {
	return func(test *testHelper) {
		ctx := test.TestCtx()
		contentTest(ctx, test, c1, c2)
	}
}

func TestRefreshType_S2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFile, createSmallFile))
}

func TestRefreshType_M2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFile, createMediumFile))
}

func TestRefreshType_L2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFile, createLargeFile))
}

func TestRefreshType_VL2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFile, createVeryLargeFile))
}

func TestRefreshType_S2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFile, createSmallFile))
}

func TestRefreshType_M2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFile, createMediumFile))
}

func TestRefreshType_L2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFile, createLargeFile))
}

func TestRefreshType_VL2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFile, createVeryLargeFile))
}

func TestRefreshType_S2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFile, createSmallFile))
}

func TestRefreshType_M2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFile, createMediumFile))
}

func TestRefreshType_L2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFile, createLargeFile))
}

func TestRefreshType_VL2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFile, createVeryLargeFile))
}

func TestRefreshType_S2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFile, createSmallFile))
}

func TestRefreshType_M2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFile, createMediumFile))
}

func TestRefreshType_L2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFile, createLargeFile))
}

func TestRefreshType_VL2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFile, createVeryLargeFile))
}

func TestRefreshType_H2H_S2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlink, createSmallFile))
}

func TestRefreshType_H2H_S2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlink, createMediumFile))
}

func TestRefreshType_H2H_S2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlink, createLargeFile))
}

func TestRefreshType_H2H_S2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlink, createVeryLargeFile))
}

func TestRefreshType_S2H_S(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlink,
		purgeHardlinkThenCreate(createSmallFile)))
}

func GenTestRefreshType_MediumAndLarge2HardlinkToSmall(
	workspaceRootHardlinks bool) func(*testHelper) {

	return func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		content1 := "original content"
		content2 := "CONTENT2"
		content3 := "the third"

		// Exactly one of the two legs of the hardlink will reside
		// in workspaceroot
		name := "testFile"
		linkname := name
		if !workspaceRootHardlinks {
			utils.MkdirAll(workspace+"/subdir", 0777)
			name = "subdir/" + name
		} else {
			utils.MkdirAll(workspace+"/linkdir", 0777)
			linkname = "linkdir/" + linkname
		}
		fullname := workspace + "/" + name
		fulllinkname := workspace + "/" + linkname

		fd, err := syscall.Creat(fullname, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullname, content1))
		test.AssertNoErr(syscall.Link(fullname, fulllinkname))
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		test.AssertNoErr(syscall.Unlink(fullname))
		test.AssertNoErr(syscall.Unlink(fulllinkname))
		test.AssertNoErr(createMediumFile(fullname, content2))
		test.AssertNoErr(createLargeFile(fulllinkname, content3))
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		file1, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file1, content2)

		file2, err := os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file2, content3)

		refreshTestNoRemount(ctx, test, workspace, newRootId2,
			newRootId1)

		// file1 was the source of the link() syscall, so it must
		// have the new content
		verifyContentStartsWith(test, file1, content1)
		test.AssertNoErr(file1.Close())

		// file2 is now orphaned and must have its old content
		verifyContentStartsWith(test, file2, content3)
		test.AssertNoErr(file2.Close())

		// but after grabbing a new file handle, both should
		// have valid content
		file1, err = os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		nmatches := doesContentStartWith(test, file1, content1)
		test.AssertNoErr(file1.Close())
		nmatches += doesContentStartWith(test, file2, content1)
		test.AssertNoErr(file2.Close())
		test.Assert(nmatches == 2, "Hit %d matches. Expected two.",
			nmatches)

		removeTestFileNoSync(test, workspace, name)
		removeTestFileNoSync(test, workspace, linkname)
	}
}

func TestRefreshType_MediumAndLarge2HardlinkToSmallWorkspaceRoot(t *testing.T) {
	runTest(t, GenTestRefreshType_MediumAndLarge2HardlinkToSmall(true))
}

func TestRefreshType_MediumAndLarge2HardlinkToSmallSubdir(t *testing.T) {
	runTest(t, GenTestRefreshType_MediumAndLarge2HardlinkToSmall(false))
}

func GenTestRefreshType_HardlinkToSmall2MediumAndLarge(
	workspaceRootHardlinks bool) func(*testHelper) {

	return func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		content1 := "original content"
		content2 := "CONTENT2"
		content3 := "the third"

		// Exactly one of the two legs of the hardlink will reside
		// in workspaceroot
		name := "testFile"
		linkname := name + "_link"
		utils.MkdirAll(workspace+"/subdir", 0777)
		if !workspaceRootHardlinks {
			name = "subdir/" + name
		} else {
			linkname = "subdir/" + linkname
		}
		fullname := workspace + "/" + name
		fulllinkname := workspace + "/" + linkname

		fd, err := syscall.Creat(fullname, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullname, content1))
		test.AssertNoErr(syscall.Link(fullname, fulllinkname))
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		// we unlink the linkfile and keep the original file,
		// this means the original file must be chosen as the heir
		// to the inode
		test.AssertNoErr(syscall.Unlink(fulllinkname))

		// Make sure fullname is normalized
		test.remountFilesystem()
		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(fullname, &stat))

		test.AssertNoErr(createMediumFile(fullname, content2))
		test.AssertNoErr(createLargeFile(fulllinkname, content3))
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		// refresh back so that we can refresh forth
		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		// This test is doing refresh twice and makes the assumption
		// that refreshing from two regular files to hardlinks is
		// fully functional and well tested
		// (See the MediumAndLarge2HardlinkToSmall tests)

		file1, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file1, content1)

		file2, err := os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file2, content1)

		refreshTestNoRemount(ctx, test, workspace, newRootId1,
			newRootId2)

		// The hardlink inode must be claimed by file1
		verifyContentStartsWith(test, file1, content2)
		test.AssertNoErr(file1.Close())

		// file2 was using the same inode and will be pointing to
		// the content of file1 after refresh, i.e. content2
		verifyContentStartsWith(test, file2, content2)
		test.AssertNoErr(file2.Close())

		// but after grabbing a new file handle, both should have
		// valid content
		file1, err = os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		nmatches := doesContentStartWith(test, file1, content2)
		test.AssertNoErr(file1.Close())
		nmatches += doesContentStartWith(test, file2, content3)
		test.Assert(nmatches == 2, "Hit %d matches. Expected two.",
			nmatches)
		test.AssertNoErr(file2.Close())

		removeTestFileNoSync(test, workspace, name)
		removeTestFileNoSync(test, workspace, linkname)
	}
}

func TestRefreshType_HardlinkToSmall2MediumAndLargeWorkspaceRoot(t *testing.T) {
	runTest(t, GenTestRefreshType_HardlinkToSmall2MediumAndLarge(true))
}

func TestRefreshType_HardlinkToSmall2MediumAndLargeSubdir(t *testing.T) {
	runTest(t, GenTestRefreshType_HardlinkToSmall2MediumAndLarge(false))
}

func TestRefreshXattrsRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()

		testfile := "test"
		attr := "user.data"
		content := []byte("extendedattributedata")

		newRootId1 := createTestFile(test, workspace, testfile, 1000)
		verifyNoXattr(test, workspace, testfile, attr)

		newRootId2 := setXattrTestFile(test, workspace, testfile,
			attr, content)
		verifyXattr(test, workspace, testfile, attr, content)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		verifyNoXattr(test, workspace, testfile, attr)
	})
}

func TestRefreshXattrsAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()

		testfile := "test"
		attr := "user.data"
		content := []byte("extendedattributedata")

		createTestFileNoSync(test, workspace, testfile, 1000)
		newRootId1 := setXattrTestFile(test, workspace, testfile,
			attr, content)
		verifyXattr(test, workspace, testfile, attr, content)
		newRootId2 := delXattrTestFile(test, workspace, testfile, attr)
		verifyNoXattr(test, workspace, testfile, attr)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		verifyXattr(test, workspace, testfile, attr, content)
	})
}

func TestRefreshSymlinkRemovalOrphaned(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		ctx := test.TestCtx()

		newRootId1 := createTestFile(test, workspace, testfile, 1000)
		newRootId2 := createTestSymlink(test, workspace, testfile,
			testfile+"_symlink")

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 1000)

		const O_PATH = 010000000
		fd, err := syscall.Open(fullname+"_symlink",
			O_PATH|syscall.O_NOFOLLOW, 0)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		assertFileIsOfSize(test, fullname, 1000)
		assertNoFile(test, fullname+"_symlink")

		var stat syscall.Stat_t
		err = syscall.Fstat(fd, &stat)
		test.AssertNoErr(err)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFLNK
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.Assert(stat.Mode == expectedPermissions,
			"Symlink has wrong permissions %x", stat.Mode)

		err = syscall.Close(fd)
		test.AssertNoErr(err)
	})
}

func TestRefreshSymlinkAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, testfile, 1000)
		newRootId1 := createTestSymlink(test, workspace, testfile,
			testfile+"_symlink")
		newRootId2 := removeTestFile(test, workspace, testfile+"_symlink")

		assertFileIsOfSize(test, fullname, 1000)
		assertNoFile(test, fullname+"_symlink")

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 1000)
	})
}

func TestRefreshType_smallfile2symlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, testfile, 1000)
		newRootId1 := createTestSymlink(test, workspace, testfile,
			testfile+"_symlink")
		removeTestFileNoSync(test, workspace, testfile+"_symlink")
		newRootId2 := createTestFile(test, workspace, testfile+"_symlink",
			2000)

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 2000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 1000)
		removeTestFileNoSync(test, workspace, testfile)
		removeTestFileNoSync(test, workspace, testfile+"_symlink")
	})
}

func TestRefreshType_symlink2smallfile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, testfile, 1000)
		newRootId1 := createTestFile(test, workspace, testfile+"_symlink",
			2000)
		removeTestFileNoSync(test, workspace, testfile+"_symlink")
		newRootId2 := createTestSymlink(test, workspace, testfile,
			testfile+"_symlink")

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 1000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		assertFileIsOfSize(test, fullname, 1000)
		assertFileIsOfSize(test, fullname+"_symlink", 2000)
		removeTestFileNoSync(test, workspace, testfile)
		removeTestFileNoSync(test, workspace, testfile+"_symlink")
	})
}

func TestRefreshType_H2H_unchanged_symlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		content1 := "original content"
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		createTestFileNoSync(test, workspace, testfile, 1000)
		createTestSymlinkNoSync(test, workspace, testfile,
			testfile+"_symlink")
		createHardlink(fullname+"_symlink", content1)
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		testutils.OverWriteFile(fullname+"_symlink", content2)
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file, content2)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		verifyContentStartsWith(test, file, content1)

		err = file.Close()
		test.AssertNoErr(err)
	})
}

func TestRefreshType_symlink2symlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile1 := "testfile1"
		testfile2 := "testfile2"
		symfile := "symfile"
		fullname := workspace + "/" + symfile
		content1 := "original content"
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		createSmallFile(workspace+"/"+testfile1, content1)
		createSmallFile(workspace+"/"+testfile2, content2)
		newRootId1 := createTestSymlink(test, workspace, testfile1, symfile)

		removeTestFileNoSync(test, workspace, symfile)
		newRootId2 := createTestSymlink(test, workspace, testfile2, symfile)

		assertFileIsOfSize(test, fullname, int64(len(content2)))
		file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		verifyContentStartsWith(test, file, content2)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		assertFileIsOfSize(test, fullname, int64(len(content1)))

		// Open() has followed the symlink already, so it must read the
		// content of testfile2
		assertOpenFileIsOfSize(test, int(file.Fd()), int64(len(content2)))
		verifyContentStartsWith(test, file, content2)
		err = file.Close()
		test.AssertNoErr(err)

		removeTestFileNoSync(test, workspace, testfile1)
		removeTestFileNoSync(test, workspace, testfile2)
		removeTestFileNoSync(test, workspace, symfile)
	})
}

func assertDeviceIs(test *testHelper, fullname string, device int) {
	var stat syscall.Stat_t
	err := syscall.Stat(fullname, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Rdev == uint64(device),
		"Expected device %x got %x", device, stat.Rdev)
}

func TestRefreshType_special2special(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullname := workspace + "/" + testfile
		ctx := test.TestCtx()

		newRootId1 := createTestSpecialFile(test, workspace, testfile,
			0xdead)
		assertDeviceIs(test, fullname, 0xdead)
		removeTestFileNoSync(test, workspace, testfile)

		newRootId2 := createTestSpecialFile(test, workspace, testfile,
			0xbeef)
		assertDeviceIs(test, fullname, 0xbeef)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		assertDeviceIs(test, fullname, 0xdead)
		removeTestFileNoSync(test, workspace, testfile)
	})
}

func GenTestRefresh_Hardlink2HardlinkWithLinkIdChange(
	workspaceRootHardlinks bool) func(*testHelper) {

	return func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		content1 := "original content"
		content2 := "CONTENT2"

		// Exactly one of the two legs of the hardlink will reside
		// in workspaceroot
		name := "testFile"
		linkname := name
		utils.MkdirAll(workspace+"/subdir", 0777)
		if !workspaceRootHardlinks {
			name = "subdir/" + name
		} else {
			linkname = "subdir/" + linkname
		}
		fullname := workspace + "/" + name
		fulllinkname := workspace + "/" + linkname

		fd, err := syscall.Creat(fullname, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullname, content1))
		test.AssertNoErr(syscall.Link(fullname, fulllinkname))
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		test.AssertNoErr(syscall.Unlink(fullname))
		test.AssertNoErr(syscall.Unlink(fulllinkname))
		fd, err = syscall.Creat(fulllinkname, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(syscall.Link(fulllinkname, fullname))
		test.AssertNoErr(createLargeFile(fulllinkname, content2))
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		file1, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err := os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2,
			newRootId1)

		// The orphan files must have the old content
		verifyContentStartsWith(test, file1, content2)
		test.AssertNoErr(file1.Close())
		verifyContentStartsWith(test, file2, content2)
		test.AssertNoErr(file2.Close())

		// Re-opening the files should result in getting the new content
		file1, err = os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		verifyContentStartsWith(test, file1, content1)
		test.AssertNoErr(file1.Close())
		verifyContentStartsWith(test, file2, content1)
		test.AssertNoErr(file2.Close())

		removeTestFileNoSync(test, workspace, name)
		removeTestFileNoSync(test, workspace, linkname)
	}
}

func TestRefresh_Hardlink2HardlinkWithLinkIdChangeSrcWorkspaceroot(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2HardlinkWithLinkIdChange(true))
}

func TestRefresh_Hardlink2HardlinkWithLinkIdChangeSrcSubdir(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2HardlinkWithLinkIdChange(false))
}

func GenTestRefresh_Hardlink2Hardlink_unlinkAndRelink(
	workspaceRootHardlinks bool) func(*testHelper) {

	return func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		content1 := "original content"
		content2 := "CONTENT2"

		// Exactly one of the two legs of the hardlink will reside
		// in workspaceroot
		name := "testFile"
		linkname := name
		utils.MkdirAll(workspace+"/subdir", 0777)
		if !workspaceRootHardlinks {
			name = "subdir/" + name
		} else {
			linkname = "subdir/" + linkname
		}
		fullname := workspace + "/" + name
		fulllinkname := workspace + "/" + linkname

		fd, err := syscall.Creat(fullname, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullname, content1))
		test.AssertNoErr(syscall.Link(fullname, fulllinkname))
		test.SyncAllWorkspaces()
		newRootId1 := getRootId(test, workspace)

		wsr, cleanup := test.getWorkspaceRoot(workspace)
		defer cleanup()

		inode := test.getInode(fullname)
		isHardlink, linkId1 := wsr.checkHardlink(inode.inodeNum())
		test.Assert(isHardlink, "testfile is not a hardlin.")

		test.AssertNoErr(syscall.Unlink(fullname))
		// Make sure fulllinkname is normalized
		test.remountFilesystem()
		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(fulllinkname, &stat))

		test.AssertNoErr(syscall.Link(fulllinkname, fullname))
		test.AssertNoErr(createLargeFile(fulllinkname, content2))
		test.SyncAllWorkspaces()
		newRootId2 := getRootId(test, workspace)

		inode = test.getInode(fullname)
		isHardlink, linkId2 := wsr.checkHardlink(inode.inodeNum())
		test.Assert(isHardlink, "testfile is not a hardlin.")

		test.Assert(linkId1 == linkId2,
			"hardlinkId changed after unlink and relink %d vs. %d",
			linkId1, linkId2)

		file1, err := os.OpenFile(fullname, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err := os.OpenFile(fulllinkname, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2,
			newRootId1)

		verifyContentStartsWith(test, file1, content1)
		test.AssertNoErr(file1.Close())
		verifyContentStartsWith(test, file2, content1)
		test.AssertNoErr(file2.Close())

		removeTestFileNoSync(test, workspace, name)
		removeTestFileNoSync(test, workspace, linkname)
	}
}

func TestRefresh_Hardlink2Hardlink_unlinkAndRelinkSrcWorkspaceroot(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2Hardlink_unlinkAndRelink(true))
}

func TestRefresh_Hardlink2Hardlink_unlinkAndRelinkSrcSubdir(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2Hardlink_unlinkAndRelink(false))
}
