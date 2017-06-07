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
	test.Log("new rootID %s", newRootId.Text())

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

func refreshHardlinkAdditionTestGen(rmLink bool) func(*testHelper) {
	return func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()
		oldRootId := createTestFile(test, workspace, "otherfile", 1000)
		createTestFile(test, workspace, name, 1000)
		newRootId1 := linkTestFile(test, workspace, name, linkfile)
		if rmLink {
			removeTestFileNoSync(test, workspace, linkfile)
		}
		newRootId2 := removeTestFile(test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(test, workspace, name)
		newRootId3 := removeTestFile(test, workspace, linkfile)

		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	}
}

func TestRefreshHardlinkAddition1(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(false))
}

func TestRefreshHardlinkAddition2(t *testing.T) {
	runTest(t, refreshHardlinkAdditionTestGen(true))
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

func TestRefreshNlink2To3(t *testing.T) {
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

func TestRefreshChangeTypeDirToHardlink(t *testing.T) {
	t.Skip() // TODO
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

func readFirstNBytes(test *testHelper, name string, n int) string {
	file, err := os.Open(name)
	test.AssertNoErr(err)
	newContent := make([]byte, n)
	_, err = io.ReadFull(file, newContent)
	test.AssertNoErr(err)
	err = file.Close()
	test.AssertNoErr(err)
	return string(newContent)
}

func contentTest(ctx *ctx, test *testHelper,
	content string,
	create1 func(string, string) error,
	create2 func(string) error) {

	workspace := test.NewWorkspace()
	utils.MkdirAll(workspace+"/subdir", 0777)
	name := "subdir/testFile"
	fullname := workspace + "/" + name

	err := create1(fullname, content)
	test.AssertNoErr(err)
	test.SyncAllWorkspaces()
	newRootId1 := getRootId(test, workspace)

	err = create2(fullname)
	test.AssertNoErr(err)
	test.SyncAllWorkspaces()
	newRootId2 := getRootId(test, workspace)

	file, err := os.OpenFile(fullname, os.O_RDWR, 0777)
	test.AssertNoErr(err)
	readFirstNBytes(test, fullname, len(content))

	refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

	newRootId3 := getRootId(test, workspace)
	test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

	err = file.Close()
	test.AssertNoErr(err)

	newContent := readFirstNBytes(test, fullname, len(content))
	test.Assert(content == newContent,
		fmt.Sprintf("content mismatch %d", len(newContent)))
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

func createHardlinkWithContent(name string, content string) error {
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

func createSmallFileWithContent(name string, content string) error {
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

func createSmallFile(name string) error {
	return createSmallFileWithContent(name, "small file content")
}

func createMediumFile(name string) error {
	size := int64(quantumfs.MaxMediumFileSize()) -
		int64(quantumfs.MaxBlockSize)
	return createSparseFile(name, size)
}

func createMediumFileWithContent(name string, content string) error {
	err := createMediumFile(name)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func createLargeFile(name string) error {
	size := int64(quantumfs.MaxMediumFileSize()) +
		int64(quantumfs.MaxBlockSize)
	return createSparseFile(name, size)
}

func createLargeFileWithContent(name string, content string) error {
	err := createLargeFile(name)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func createVeryLargeFile(name string) error {
	size := int64(quantumfs.MaxLargeFileSize()) +
		int64(quantumfs.MaxBlockSize)
	return createSparseFile(name, size)
}

func createVeryLargeFileWithContent(name string, content string) error {
	err := createVeryLargeFile(name)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func contentCheckTestGen(c1 func(string, string) error,
	c2 func(string) error) func(*testHelper) {

	return func(test *testHelper) {
		ctx := test.TestCtx()
		contentTest(ctx, test, "original content", c1, c2)
	}
}

func TestRefreshType_S2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFileWithContent,
		createSmallFile))
}

func TestRefreshType_M2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFileWithContent,
		createMediumFile))
}

func TestRefreshType_L2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFileWithContent,
		createLargeFile))
}

func TestRefreshType_VL2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createSmallFileWithContent,
		createVeryLargeFile))
}

func TestRefreshType_S2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFileWithContent,
		createSmallFile))
}

func TestRefreshType_M2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFileWithContent,
		createMediumFile))
}

func TestRefreshType_L2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFileWithContent,
		createLargeFile))
}

func TestRefreshType_VL2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createMediumFileWithContent,
		createVeryLargeFile))
}

func TestRefreshType_S2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFileWithContent,
		createSmallFile))
}

func TestRefreshType_M2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFileWithContent,
		createMediumFile))
}

func TestRefreshType_L2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFileWithContent,
		createLargeFile))
}

func TestRefreshType_VL2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createLargeFileWithContent,
		createVeryLargeFile))
}

func TestRefreshType_S2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFileWithContent,
		createSmallFile))
}

func TestRefreshType_M2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFileWithContent,
		createMediumFile))
}

func TestRefreshType_L2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFileWithContent,
		createLargeFile))
}

func TestRefreshType_VL2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createVeryLargeFileWithContent,
		createVeryLargeFile))
}

func TestRefreshType_H2H_S2S(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlinkWithContent,
		createSmallFile))
}

func TestRefreshType_H2H_S2M(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlinkWithContent,
		createMediumFile))
}

func TestRefreshType_H2H_S2L(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlinkWithContent,
		createLargeFile))
}

func TestRefreshType_H2H_S2VL(t *testing.T) {
	runTest(t, contentCheckTestGen(createHardlinkWithContent,
		createVeryLargeFile))
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
