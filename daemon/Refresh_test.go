// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

func (test *testHelper) markImmutable(ctx *ctx, workspace string) {
	defer ctx.FuncIn("markImmutable", "%s", workspace).Out()
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	_, exists := ctx.qfs.workspaceMutability[workspace]
	test.Assert(exists, "bad workspace name %s", workspace)
	ctx.qfs.workspaceMutability[workspace] = workspaceImmutable
}

func (test *testHelper) markMutable(ctx *ctx, workspace string) {
	defer ctx.FuncIn("markMutable", "%s", workspace).Out()
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	_, exists := ctx.qfs.workspaceMutability[workspace]
	test.Assert(exists, "bad workspace name %s", workspace)
	ctx.qfs.workspaceMutability[workspace] = workspaceMutable
}

func (th *testHelper) waitForNRefresh(workspace string, n int) {
	th.WaitForNLogStrings(fmt.Sprintf("Workspace Refreshing %s", workspace), n,
		"Workspace not refreshed")
	th.WaitForNLogStrings("Out-- WorkspaceRoot::refresh", n,
		"Workspace refresh not finished")
}

func refreshTestNoRemount(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	ts, ns, ws := test.getWorkspaceComponents(workspace)
	_, nonce := test.workspaceRootId(ts, ns, ws)
	workspaceName := ts + "/" + ns + "/" + ws

	_, cleanup := test.GetWorkspaceRoot(workspace)
	// This will prevent the workspace from getting uninstantiated and
	// losing the updates
	defer cleanup()

	test.markImmutable(ctx, workspaceName)
	test.advanceWorkspace(workspace, nonce, src, dst)
	test.WaitForRefreshTo(workspaceName, dst)
	test.markMutable(ctx, workspaceName)
}

func refreshTest(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	ts, ns, ws := test.getWorkspaceComponents(workspace)
	_, nonce := test.workspaceRootId(ts, ns, ws)

	workspaceName := ts + "/" + ns + "/" + ws
	test.markImmutable(ctx, workspaceName)
	test.remountFilesystem()

	// Ensure the WSR is instantiated over the refresh
	dir, err := os.Open(workspace)
	test.AssertNoErr(err)
	defer dir.Close()

	test.advanceWorkspace(workspaceName, nonce, src, dst)
	test.WaitForRefreshTo(workspaceName, dst)
	test.markMutable(ctx, workspaceName)
}

func TestRefreshFileAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		newRootId1 := test.createFileSync(workspace, name, 1000)
		newRootId2 := test.removeFileSync(workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		test.removeFile(workspace, name)
	})
}

func TestRefreshFileRewrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		ctx := test.TestCtx()

		test.createFile(workspace, "otherfile", 1000)
		newRootId1 := test.createFileSync(workspace, name, 1000)
		newRootId2 := test.createFileSync(workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := test.getRootId(workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		test.removeFile(workspace, name)
	})
}

func TestRefreshFileRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		ctx := test.TestCtx()

		test.createFile(workspace, "otherfile", 1000)
		test.createFile(workspace, name, 1000)
		newRootId1 := test.removeFileSync(workspace, name)
		newRootId2 := test.createFileSync(workspace, name, 2000)

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

		oldRootId := test.createFileSync(workspace, "otherfile", 1000)
		test.createFile(workspace, name, 1000)

		content := "content to be verified"
		err := CreateSmallFile(workspace+"/"+name, content)
		test.AssertNoErr(err)

		if link1 {
			test.createFile(workspace, name+".link1", 1000)
			test.linkFile(workspace, name+".link1",
				linkfile+".link1")
		}
		newRootId1 := test.linkFileSync(workspace, name, linkfile)
		if rmLink0 {
			test.removeFile(workspace, linkfile)
		}
		if link2 {
			test.createFile(workspace, name+".link2", 1000)
			test.linkFile(workspace, name+".link2",
				linkfile+".link2")
		}

		newRootId2 := test.removeFileSync(workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		file, err := os.OpenFile(workspace+"/"+name, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content)
		test.AssertNoErr(file.Close())

		if link1 {
			test.removeFile(workspace, name+".link1")
			test.removeFile(workspace, linkfile+".link1")
		}

		_, err = os.Stat(workspace + "/" + linkfile + ".link2")
		test.AssertErr(err)

		test.removeFile(workspace, name)
		newRootId3 := test.removeFileSync(workspace, linkfile)

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
		fullName := workspace + "/testFile"
		content := "original content"
		appendContent := " appended."
		ctx := test.TestCtx()

		newRootId1 := test.createFileSync(workspace, "otherfile", 1000)
		CreateHardlink(fullName, content)
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content)
		test.assertOpenFileIsOfSize(int(file.Fd()), int64(len(content)))

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file, content)
		test.assertNoFile(fullName)
		test.assertOpenFileIsOfSize(int(file.Fd()), int64(len(content)))
		_, err = file.Write([]byte(appendContent))
		test.AssertNoErr(err)
		test.assertNoFile(fullName)
		test.assertOpenFileIsOfSize(int(file.Fd()),
			int64(len(content)+len(appendContent)))
		test.verifyContentStartsWith(file, content+appendContent)

		test.AssertNoErr(file.Close())
	})
}

func TestRefreshHardlinkRemoval(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"
		ctx := test.TestCtx()

		newRootId1 := test.createFileSync(workspace, name, 1000)
		newRootId2 := test.linkFileSync(workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		test.removeFile(workspace, name)
		linkname := workspace + "/" + linkfile
		err := os.Remove(linkname)
		test.Assert(err != nil, "The linkfile must not exist after refresh")
	})
}

func refreshRenameToHardlinkGen(subdir bool) func(*testHelper) {
	return func(test *testHelper) {
		workspace := test.NewWorkspace()

		name := "testFile"
		leg1 := "leg1"
		leg2 := "leg2"

		if subdir {
			utils.MkdirAll(workspace+"/subdir", 0777)
			name = "subdir/" + name
		}

		content1 := "original content"
		appendContent := " appended."
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		test.AssertNoErr(CreateSmallFile(workspace+"/"+leg1, content1))
		newRootId1 := test.linkFileSync(workspace, leg1, leg2)

		file, err := os.OpenFile(workspace+"/"+leg1, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		test.AssertNoErr(testutils.OverWriteFile(workspace+"/"+leg1,
			content2))
		test.removeFile(workspace, leg2)
		newRootId2 := test.moveFileSync(workspace, leg1, name)

		file.Close()

		test.remountFilesystem()
		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(workspace+"/"+name, &stat))
		file, err = os.OpenFile(workspace+"/"+name, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file, content1)
		_, err = file.Write([]byte(appendContent))
		test.AssertNoErr(err)
		file.Close()

		test.remountFilesystem()
		file, err = os.OpenFile(workspace+"/"+leg1, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content1+appendContent)
		file.Close()

		test.assertNoFile(workspace + "/" + name)
		test.removeFile(workspace, leg1)
		test.removeFile(workspace, leg2)
	}
}

func TestRefreshRenameToHardlinkSubdir(t *testing.T) {
	runTest(t, refreshRenameToHardlinkGen(true))
}

func TestRefreshRenameToHardlinkWsr(t *testing.T) {
	runTest(t, refreshRenameToHardlinkGen(false))
}

func TestRefreshNlinkDrop(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := test.createFileSync(workspace, "otherfile", 1000)
		newRootId1 := test.createFileSync(workspace, name, 1000)
		var newRootId2 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId2 = test.linkFileSync(workspace, name,
				linkfile)
		}

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := test.removeFileSync(workspace, name)
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlinkBump(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		oldRootId := test.createFileSync(workspace, "otherfile", 1000)
		test.createFile(workspace, name, 1000)
		var newRootId1 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId1 = test.linkFileSync(workspace, name,
				linkfile)
		}

		newRootId2 := test.removeFileSync(workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := test.removeFileSync(workspace, name)

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId3 = test.removeFileSync(workspace, linkfile)
		}
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlink3To2(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		test.createFile(workspace, name, 1000)
		newRootId1 := test.linkFileSync(workspace, name, "link1")
		newRootId2 := test.linkFileSync(workspace, name, "link2")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		// Create and delete a temporary file to make sure a new rootId
		// is published
		test.createFile(workspace, name+".tmp", 1000)
		newRootId3 := test.removeFileSync(workspace, name+".tmp")

		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")
		test.removeFile(workspace, "link1")
		test.removeFile(workspace, name)
	})
}

func TestRefreshNlink2To3(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		test.createFile(workspace, name, 1000)
		test.linkFile(workspace, name, "link1")
		newRootId1 := test.linkFileSync(workspace, name, "link2")
		newRootId2 := test.removeFileSync(workspace, "link2")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		// Create and delete a temporary file to make sure a new rootId
		// is published
		test.createFile(workspace, name+".tmp", 1000)
		newRootId3 := test.removeFileSync(workspace, name+".tmp")

		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")
		test.removeFile(workspace, "link2")
		test.removeFile(workspace, "link1")
		test.removeFile(workspace, name)
	})
}

func TestRefreshOpenFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		fullName := workspace + "/" + name

		ctx := test.TestCtx()
		newRootId1 := test.createFileSync(workspace, name, 1000)
		newRootId2 := test.createFileSync(workspace, name, 2000)

		file, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.assertFileIsOfSize(fullName, 3000)
		test.assertOpenFileIsOfSize(int(file.Fd()), 3000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := test.getRootId(workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		test.assertOpenFileIsOfSize(int(file.Fd()), 1000)
		test.assertFileIsOfSize(fullName, 1000)

		test.AssertNoErr(file.Close())
		test.removeFile(workspace, name)
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
			test.createFile(workspace, name, 1000)
		}
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		for i := 0; i < nfiles; i++ {
			name := fmt.Sprintf("%s/%d/%d", pardir, i%ndirs, i)
			test.removeFile(workspace, name)
		}
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		test.WaitForLogString("Adding uninstantiated",
			"There are no uninstantiated inodes")

		for i := 0; i < nfiles; i++ {
			name := fmt.Sprintf("%s/%d/%d", pardir, i%ndirs, i)
			test.removeFile(workspace, name)
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
		newRootId1 := test.createFileSync(workspace, subdir+"/"+filename, 0)
		// remount to make sure the corresponding inode is uninstantiated
		test.remountFilesystem()
		newRootId2 := test.createFileSync(workspace, subdir+"/otherfile", 0)

		var stat syscall.Stat_t
		err := syscall.Stat(fullfilename, &stat)
		test.AssertNoErr(err)
		inodeId := stat.Ino

		ctx := test.TestCtx()
		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.AssertNoErr(syscall.Stat(fullfilename, &stat))
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

		test.createFile(workspace, name, 1000)
		newRootId1 := test.linkFileSync(workspace, name, linkfile)
		test.removeFile(workspace, name)
		utils.MkdirAll(workspace+"/"+name, 0777)
		test.SyncAllWorkspaces()
		test.dirtyAndSync(workspace)

		newRootId2 := test.getRootId(workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		test.removeFile(workspace, name)
		test.removeFile(workspace, linkfile)
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
		newRootId1 := test.getRootId(workspace)

		err := syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)

		test.AssertNoErr(syscall.Rmdir(workspace + "/" + linkfile))
		test.createFile(workspace, name, 1000)
		newRootId2 := test.linkFileSync(workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		test.createFile(workspace, name+"/subfile", 1000)
		test.removeFile(workspace, name+"/subfile")
		test.AssertNoErr(syscall.Rmdir(workspace + "/" + name))
		test.AssertNoErr(syscall.Rmdir(workspace + "/" + linkfile))
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
		newRootId1 := test.createFileSync(workspace, filename, 1000)
		_, err = os.Stat(fullfilename)
		test.AssertNoErr(err)

		test.AssertNoErr(os.RemoveAll(fulldirame))
		test.AssertNoErr(os.RemoveAll(fullfilename))
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

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

		test.createFile(workspace, name, 1000)
		newRootId1 := test.removeFileSync(workspace, name)
		newRootId2 := test.createFileSync(workspace, name, 1000)

		f, err := os.OpenFile(workspace+"/"+name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.AssertNoErr(f.Close())
		_, err = os.OpenFile(workspace+"/"+name, os.O_RDONLY, 0777)
		test.AssertErr(err)
	})
}

func TestRefreshChangeTypeDirToFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		ctx := test.TestCtx()

		newRootId1 := test.createFileSync(workspace, name, 1000)

		test.removeFile(workspace, name)
		utils.MkdirAll(workspace+"/"+name, 0777)
		utils.MkdirAll(workspace+"/"+name+"/subdir", 0777)
		test.createFile(workspace, name+"/subfile", 1000)
		test.createFile(workspace, name+"/subdir/subfile", 1000)
		utils.MkdirAll(workspace+"/"+name+"/subdir/subdir1", 0777)

		subfile1name := workspace + "/" + name + "/subfile"
		subfile1, err := os.OpenFile(subfile1name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		subfile2name := workspace + "/" + name + "/subdir/subfile"
		subfile2, err := os.OpenFile(subfile2name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.AssertNoErr(subfile1.Close())
		test.AssertNoErr(subfile2.Close())
		test.removeFile(workspace, name)

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
		newRootId1 := test.getRootId(workspace)
		test.removeFile(workspace, name)
		newRootId2 := test.createFileSync(workspace, name, 1000)
		test.Assert(!newRootId2.IsEqualTo(newRootId1),
			"no changes to the rootId")

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		err := syscall.Rmdir(workspace + "/" + name)
		test.AssertNoErr(err)
	})
}

type createFunc func(name string, content string) error

func contentTest(ctx *ctx, test *testHelper, c1 createFunc, c2 createFunc) {
	workspace := test.NewWorkspace()
	content1 := "original content"
	content2 := "CONTENT2"
	utils.MkdirAll(workspace+"/subdir", 0777)
	name := "subdir/testFile"
	fullName := workspace + "/" + name

	err := c1(fullName, content1)
	test.AssertNoErr(err)
	test.SyncAllWorkspaces()
	newRootId1 := test.getRootId(workspace)

	test.AssertNoErr(c2(fullName, content2))
	test.SyncAllWorkspaces()
	newRootId2 := test.getRootId(workspace)

	file, err := os.OpenFile(fullName, os.O_RDWR, 0777)
	test.AssertNoErr(err)
	test.verifyContentStartsWith(file, content2)

	refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

	newRootId3 := test.getRootId(workspace)
	test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

	test.verifyContentStartsWith(file, content1)
	test.AssertNoErr(file.Close())

	file, err = os.Open(fullName)
	test.AssertNoErr(err)
	test.verifyContentStartsWith(file, content1)
	test.AssertNoErr(file.Close())
	test.removeFile(workspace, name)
}

func purgeHardlinkThenCreate(create createFunc) createFunc {
	return func(name string, content string) error {
		if err := syscall.Unlink(name + "_link"); err != nil {
			return err
		}
		return create(name, content)
	}
}

func contentCheckTestGen(c1 createFunc, c2 createFunc) func(*testHelper) {
	return func(test *testHelper) {
		ctx := test.TestCtx()
		contentTest(ctx, test, c1, c2)
	}
}

func TestRefreshType_SmallFile2SmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateSmallFile,
		CreateSmallFile))
}

func TestRefreshType_MediumFile2SmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateSmallFile,
		CreateMediumFile))
}

func TestRefreshType_LargeFile2SmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateSmallFile,
		CreateLargeFile))
}

func TestRefreshType_VeryLargeFile2SmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateSmallFile,
		CreateVeryLargeFile))
}

func TestRefreshType_SmallFile2MediumFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateMediumFile,
		CreateSmallFile))
}

func TestRefreshType_MediumFile2MediumFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateMediumFile,
		CreateMediumFile))
}

func TestRefreshType_LargeFile2MediumFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateMediumFile,
		CreateLargeFile))
}

func TestRefreshType_VeryLargeFile2MediumFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateMediumFile,
		CreateVeryLargeFile))
}

func TestRefreshType_SmallFile2LargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateLargeFile,
		CreateSmallFile))
}

func TestRefreshType_MediumFile2LargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateLargeFile,
		CreateMediumFile))
}

func TestRefreshType_LargeFile2LargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateLargeFile,
		CreateLargeFile))
}

func TestRefreshType_VeryLargeFile2LargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateLargeFile,
		CreateVeryLargeFile))
}

func TestRefreshType_SmallFile2VeryLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateVeryLargeFile,
		CreateSmallFile))
}

func TestRefreshType_MediumFile2VeryLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateVeryLargeFile,
		CreateMediumFile))
}

func TestRefreshType_LargeFile2VeryLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateVeryLargeFile,
		CreateLargeFile))
}

func TestRefreshType_VeryLargeFile2VeryLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateVeryLargeFile,
		CreateVeryLargeFile))
}

func TestRefreshType_HardlinkToSmallFile2HardlinkToSmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateHardlink,
		CreateSmallFile))
}

func TestRefreshType_HardlinkToSmallFile2HardlinkToMediumFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateHardlink,
		CreateMediumFile))
}

func TestRefreshType_HardlinkToSmallFile2HardlinkToLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateHardlink,
		CreateLargeFile))
}

func TestRefreshType_HardlinkToSmallFile2HardlinkToVeryLargeFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateHardlink,
		CreateVeryLargeFile))
}

func TestRefreshType_SmallFile2HardlinkToSmallFile(t *testing.T) {
	runTest(t, contentCheckTestGen(CreateHardlink,
		purgeHardlinkThenCreate(CreateSmallFile)))
}

func GenTestRefreshType_MediumAndLarge2HardlinkToSmallFile(
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
		fullName := workspace + "/" + name
		fullLinkName := workspace + "/" + linkname

		fd, err := syscall.Creat(fullName, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullName, content1))
		test.AssertNoErr(syscall.Link(fullName, fullLinkName))
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		// unlink and recreate the link file so that it gets a new fileId
		test.AssertNoErr(syscall.Unlink(fullLinkName))

		test.AssertNoErr(CreateMediumFile(fullName, content2))
		test.AssertNoErr(CreateLargeFile(fullLinkName, content3))

		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file1, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file1, content2)

		file2, err := os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file2, content3)

		refreshTestNoRemount(ctx, test, workspace, newRootId2,
			newRootId1)

		// file1 was only modified so it must have the new content
		test.verifyContentStartsWith(file1, content1)
		test.AssertNoErr(file1.Close())

		// file2 was deleted and re-created so it must be orphaned
		// and must have its old content
		test.verifyContentStartsWith(file2, content3)
		test.AssertNoErr(file2.Close())

		// After grabbing a new file handle, both should
		// have valid content
		file1, err = os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		test.verifyContentStartsWith(file1, content1)
		test.AssertNoErr(file1.Close())
		test.verifyContentStartsWith(file2, content1)
		test.AssertNoErr(file2.Close())

		test.removeFile(workspace, name)
		test.removeFile(workspace, linkname)
	}
}

func TestRefreshType_MediumAndLarge2HardlinkToSmallFileWorkspaceRoot(t *testing.T) {
	runTest(t, GenTestRefreshType_MediumAndLarge2HardlinkToSmallFile(true))
}

func TestRefreshType_MediumAndLarge2HardlinkToSmallFileSubdir(t *testing.T) {
	runTest(t, GenTestRefreshType_MediumAndLarge2HardlinkToSmallFile(false))
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
		fullName := workspace + "/" + name
		fullLinkName := workspace + "/" + linkname

		fd, err := syscall.Creat(fullName, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullName, content1))
		test.AssertNoErr(syscall.Link(fullName, fullLinkName))
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		// we unlink the linkfile and keep the original file,
		// this means the original file must be chosen as the heir
		// to the inode
		test.AssertNoErr(syscall.Unlink(fullLinkName))

		// Make sure fullName is normalized
		test.remountFilesystem()
		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(fullName, &stat))

		test.AssertNoErr(CreateMediumFile(fullName, content2))
		test.AssertNoErr(CreateLargeFile(fullLinkName, content3))
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		// refresh back so that we can refresh forth
		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		// This test is doing refresh twice and makes the assumption
		// that refreshing from two regular files to hardlinks is
		// fully functional and well tested
		// (See the MediumAndLarge2HardlinkToSmall tests)

		file1, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file1, content1)

		file2, err := os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file2, content1)

		refreshTestNoRemount(ctx, test, workspace, newRootId1, newRootId2)

		// The hardlink inode must be claimed by file1
		test.verifyContentStartsWith(file1, content2)
		test.AssertNoErr(file1.Close())

		// file2 was using the same inode and will be pointing to
		// the content of file1 after refresh, i.e. content2
		test.verifyContentStartsWith(file2, content2)
		test.AssertNoErr(file2.Close())

		// but after grabbing a new file handle, both should have
		// valid content
		file1, err = os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		test.verifyContentStartsWith(file1, content2)
		test.AssertNoErr(file1.Close())
		test.verifyContentStartsWith(file2, content3)
		test.AssertNoErr(file2.Close())

		test.removeFile(workspace, name)
		test.removeFile(workspace, linkname)
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

		newRootId1 := test.createFileSync(workspace, testfile, 1000)
		test.verifyNoXattr(workspace, testfile, attr)

		newRootId2 := test.setXattrSync(workspace, testfile,
			attr, content)
		test.verifyXattr(workspace, testfile, attr, content)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		test.verifyNoXattr(workspace, testfile, attr)
	})
}

func TestRefreshXattrsAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()

		testfile := "test"
		attr := "user.data"
		content := []byte("extendedattributedata")

		test.createFile(workspace, testfile, 1000)
		newRootId1 := test.setXattrSync(workspace, testfile,
			attr, content)
		test.verifyXattr(workspace, testfile, attr, content)
		newRootId2 := test.delXattrSync(workspace, testfile, attr)
		test.verifyNoXattr(workspace, testfile, attr)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		test.verifyXattr(workspace, testfile, attr, content)
	})
}

func TestRefreshSymlinkRemovalOrphaned(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		ctx := test.TestCtx()

		newRootId1 := test.createFileSync(workspace, testfile, 1000)
		newRootId2 := test.createSymlinkSync(workspace, testfile,
			testfile+"_symlink")

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 1000)

		const O_PATH = 010000000
		fd, err := syscall.Open(fullName+"_symlink",
			O_PATH|syscall.O_NOFOLLOW, 0)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.assertFileIsOfSize(fullName, 1000)
		test.assertNoFile(fullName + "_symlink")

		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Fstat(fd, &stat))

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFLNK
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.Assert(stat.Mode == expectedPermissions,
			"Symlink has wrong permissions %x", stat.Mode)

		test.AssertNoErr(syscall.Close(fd))
	})
}

func TestRefreshSymlinkAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		ctx := test.TestCtx()

		test.createFile(workspace, testfile, 1000)
		newRootId1 := test.createSymlinkSync(workspace, testfile,
			testfile+"_symlink")
		newRootId2 := test.removeFileSync(workspace, testfile+"_symlink")

		test.assertFileIsOfSize(fullName, 1000)
		test.assertNoFile(fullName + "_symlink")

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 1000)
	})
}

func TestRefreshType_smallfile2symlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		ctx := test.TestCtx()

		test.createFile(workspace, testfile, 1000)
		newRootId1 := test.createSymlinkSync(workspace, testfile,
			testfile+"_symlink")
		test.removeFile(workspace, testfile+"_symlink")
		newRootId2 := test.createFileSync(workspace, testfile+"_symlink",
			2000)

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 2000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 1000)
		test.removeFile(workspace, testfile)
		test.removeFile(workspace, testfile+"_symlink")
	})
}

func TestRefreshType_symlink2smallfile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		ctx := test.TestCtx()

		test.createFile(workspace, testfile, 1000)
		newRootId1 := test.createFileSync(workspace, testfile+"_symlink",
			2000)
		test.removeFile(workspace, testfile+"_symlink")
		newRootId2 := test.createSymlinkSync(workspace, testfile,
			testfile+"_symlink")

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 1000)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.assertFileIsOfSize(fullName, 1000)
		test.assertFileIsOfSize(fullName+"_symlink", 2000)
		test.removeFile(workspace, testfile)
		test.removeFile(workspace, testfile+"_symlink")
	})
}

func TestRefreshType_HardlinkToSymlink2HardlinkToSymlink_unchanged(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		content1 := "original content"
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		test.createFile(workspace, testfile, 1000)
		test.createSymlink(workspace, testfile, testfile+"_symlink")
		CreateHardlink(fullName+"_symlink", content1)
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		testutils.OverWriteFile(fullName+"_symlink", content2)
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content2)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file, content1)

		test.AssertNoErr(file.Close())
	})
}

func TestRefreshType_symlink2symlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile1 := "testfile1"
		testfile2 := "testfile2"
		symfile := "symfile"
		fullName := workspace + "/" + symfile
		content1 := "original content"
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		CreateSmallFile(workspace+"/"+testfile1, content1)
		CreateSmallFile(workspace+"/"+testfile2, content2)
		newRootId1 := test.createSymlinkSync(workspace, testfile1, symfile)

		test.removeFile(workspace, symfile)
		newRootId2 := test.createSymlinkSync(workspace, testfile2, symfile)

		test.assertFileIsOfSize(fullName, int64(len(content2)))
		file, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content2)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.assertFileIsOfSize(fullName, int64(len(content1)))

		// Open() has followed the symlink already, so it must read the
		// content of testfile2
		test.assertOpenFileIsOfSize(int(file.Fd()), int64(len(content2)))
		test.verifyContentStartsWith(file, content2)
		test.AssertNoErr(file.Close())

		test.removeFile(workspace, testfile1)
		test.removeFile(workspace, testfile2)
		test.removeFile(workspace, symfile)
	})
}

func assertDeviceIs(test *testHelper, fullName string, device int) {
	var stat syscall.Stat_t
	err := syscall.Stat(fullName, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Rdev == uint64(device),
		"Expected device %x got %x", device, stat.Rdev)
}

func TestRefreshType_special2special(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testfile := "testfile"
		fullName := workspace + "/" + testfile
		ctx := test.TestCtx()

		newRootId1 := test.createSpecialFileSync(workspace, testfile, 0xdead)
		assertDeviceIs(test, fullName, 0xdead)
		test.removeFile(workspace, testfile)

		newRootId2 := test.createSpecialFileSync(workspace, testfile, 0xbeef)
		assertDeviceIs(test, fullName, 0xbeef)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)
		assertDeviceIs(test, fullName, 0xdead)
		test.removeFile(workspace, testfile)
	})
}

func GenTestRefresh_Hardlink2HardlinkWithFileIdChange(
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
		fullName := workspace + "/" + name
		fullLinkName := workspace + "/" + linkname

		fd, err := syscall.Creat(fullName, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullName, content1))
		test.AssertNoErr(syscall.Link(fullName, fullLinkName))
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		test.AssertNoErr(syscall.Unlink(fullName))
		test.AssertNoErr(syscall.Unlink(fullLinkName))
		fd, err = syscall.Creat(fullLinkName, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(syscall.Link(fullLinkName, fullName))
		test.AssertNoErr(CreateLargeFile(fullLinkName, content2))
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file1, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err := os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		// The orphan files must have the old content
		test.verifyContentStartsWith(file1, content2)
		test.AssertNoErr(file1.Close())
		test.verifyContentStartsWith(file2, content2)
		test.AssertNoErr(file2.Close())

		// Re-opening the files should result in getting the new content
		file1, err = os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err = os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		test.verifyContentStartsWith(file1, content1)
		test.AssertNoErr(file1.Close())
		test.verifyContentStartsWith(file2, content1)
		test.AssertNoErr(file2.Close())

		test.removeFile(workspace, name)
		test.removeFile(workspace, linkname)
	}
}

func TestRefresh_Hardlink2HardlinkWithFileIdChangeSrcWorkspaceroot(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2HardlinkWithFileIdChange(true))
}

func TestRefresh_Hardlink2HardlinkWithFileIdChangeSrcSubdir(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2HardlinkWithFileIdChange(false))
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
		fullName := workspace + "/" + name
		fullLinkName := workspace + "/" + linkname

		fd, err := syscall.Creat(fullName, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(testutils.OverWriteFile(fullName, content1))
		test.AssertNoErr(syscall.Link(fullName, fullLinkName))
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		wsr, cleanup := test.GetWorkspaceRoot(workspace)
		defer cleanup()

		inode := test.getInode(fullName)
		isHardlink, fileId1 := wsr.hardlinkTable.checkHardlink(
			inode.inodeNum())
		test.Assert(isHardlink, "testfile is not a hardlin.")

		test.AssertNoErr(syscall.Unlink(fullName))
		// Make sure fullLinkName is normalized
		test.remountFilesystem()
		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(fullLinkName, &stat))

		test.AssertNoErr(syscall.Link(fullLinkName, fullName))
		test.AssertNoErr(CreateLargeFile(fullLinkName, content2))
		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		inode = test.getInode(fullName)
		isHardlink, fileId2 := wsr.hardlinkTable.checkHardlink(
			inode.inodeNum())
		test.Assert(isHardlink, "testfile is not a hardlin.")

		test.Assert(fileId1 == fileId2,
			"fileId changed after unlink and relink %d vs. %d",
			fileId1, fileId2)

		file1, err := os.OpenFile(fullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)
		file2, err := os.OpenFile(fullLinkName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file1, content1)
		test.AssertNoErr(file1.Close())
		test.verifyContentStartsWith(file2, content1)
		test.AssertNoErr(file2.Close())

		test.removeFile(workspace, name)
		test.removeFile(workspace, linkname)
	}
}

func TestRefresh_Hardlink2Hardlink_unlinkAndRelinkSrcWorkspaceroot(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2Hardlink_unlinkAndRelink(true))
}

func TestRefresh_Hardlink2Hardlink_unlinkAndRelinkSrcSubdir(t *testing.T) {
	runTest(t, GenTestRefresh_Hardlink2Hardlink_unlinkAndRelink(false))
}

func GenTestRefreshRename(srcName string, dstName string) func(*testHelper) {
	return func(test *testHelper) {
		workspace := test.NewWorkspace()
		dstFullName := workspace + "/" + dstName
		srcFullName := workspace + "/" + srcName
		content1 := "original content"
		content2 := "CONTENT2"
		ctx := test.TestCtx()

		utils.MkdirAll(workspace+"/"+filepath.Dir(dstName), 0777)
		CreateSmallFile(dstFullName, content1)
		test.AssertNoErr(testutils.OverWriteFile(dstFullName, content1))
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		test.moveFileSync(workspace, dstName, ".tmp")
		os.RemoveAll(srcFullName)
		utils.MkdirAll(workspace+"/"+filepath.Dir(srcName), 0777)
		test.moveFileSync(workspace, ".tmp", srcName)
		test.AssertNoErr(testutils.OverWriteFile(srcFullName, content2))

		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file, err := os.OpenFile(srcFullName, os.O_RDWR, 0777)
		test.AssertNoErr(err)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file, content1)
		test.AssertNoErr(file.Close())

		if strings.HasPrefix(dstName, srcName) {
			// src must exists as it is a directory to dst
			var stat syscall.Stat_t
			test.AssertNoErr(syscall.Stat(srcFullName, &stat))
		} else if strings.HasPrefix(srcName, dstName) {
			var stat syscall.Stat_t
			err := syscall.Stat(srcFullName, &stat)
			test.AssertErr(err)
			test.Assert(err == syscall.ENOTDIR,
				"Expected ENOTDIR, got %s", err.Error())
		} else {
			test.assertNoFile(srcFullName)
		}
		test.removeFile(workspace, dstName)
	}
}

func TestRefreshRenameFileWSR2WSR(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile", "moved"))
}

func TestRefreshRenameFileWSR2Dir(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile", "dir1/moved"))
}

func TestRefreshRenameFileDir2WSR(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir1/testFile", "moved"))
}

func TestRefreshRenameFileDir2SameDir(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir1/testFile", "dir1/moved"))
}

func TestRefreshRenameFileDir2OtherDir1(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir1/testFile", "dir2/dir3/moved"))
}

func TestRefreshRenameFileDir2OtherDir2(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir2/dir3/testFile", "dir1/moved"))
}

func TestRefreshRenameFileDir2Subdir(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir2/testFile", "dir2/dir3/moved"))
}

func TestRefreshRenameFileSubdir2Dir(t *testing.T) {
	runTest(t, GenTestRefreshRename("dir2/dir3/testFile", "dir2/moved"))
}

func TestRefreshMoveFileToDirWithSameNameWSR(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile", "testFile/testFile"))
}

func TestRefreshMoveFileFromDirWithSameNameDir(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile/testFile",
		"testFile/testFile/testFile"))
}

func TestRefreshMoveFileFromDirWithSameNameWSR(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile/testFile", "testFile"))
}

func TestRefreshMoveFileToDirWithSameNameDir(t *testing.T) {
	runTest(t, GenTestRefreshRename("testFile/testFile/testFile",
		"testFile/testFile"))
}

func TestRefreshMoveLongWayDown(t *testing.T) {
	runTest(t, GenTestRefreshRename("t", "t/t/t/t/t/t/t/t/t"))
}

func TestRefreshMoveLongWayUp(t *testing.T) {
	runTest(t, GenTestRefreshRename("t/t/t/t/t/t/t/t/t", "t/t"))
}

func TestRefreshMoveSmallFileToHardlinkToSmallFileAndBack(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ctx := test.TestCtx()
		workspace := test.NewWorkspace()
		content1 := "original content"
		content2 := "CONTENT2"
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"
		fullName := workspace + "/" + name

		err := CreateHardlink(fullName, content1)
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()
		newRootId1 := test.getRootId(workspace)

		test.removeFile(workspace, name+"_link")
		test.AssertNoErr(testutils.OverWriteFile(fullName, content2))
		test.moveFileSync(workspace, name, name+".tmp")

		test.SyncAllWorkspaces()
		newRootId2 := test.getRootId(workspace)

		file, err := os.OpenFile(fullName+".tmp", os.O_RDWR, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content2)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := test.getRootId(workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		test.verifyContentStartsWith(file, content1)
		test.AssertNoErr(file.Close())

		file, err = os.Open(fullName)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content1)

		refreshTestNoRemount(ctx, test, workspace, newRootId1, newRootId2)

		test.verifyContentStartsWith(file, content2)
		test.AssertNoErr(file.Close())

		test.assertNoFile(fullName)
		test.removeFile(workspace, name+".tmp")
	})
}

func TestRefreshDualInstances(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		c := test.TestCtx()
		mnt1 := test.qfsInstances[1].config.MountPath + "/"
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + workspaceName
		name := "testFile"

		test.markImmutable(c, workspaceName)
		api1, err := quantumfs.NewApiWithPath(mnt1 + "api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		test.assertNoFile(workspace0 + "/" + name)

		// create a file in the second instance
		test.createFile(workspace1, name, 1000)
		test.AssertNoErr(api1.SyncAll())

		test.waitForNRefresh(workspaceName, 1)

		// the file should appear in the first instance now
		file, err := os.OpenFile(workspace0+"/"+name, os.O_RDONLY, 0777)
		test.AssertNoErr(err)

		// overwrite the content of the file in workspace1
		content := "content to be verified"
		test.AssertNoErr(CreateSmallFile(workspace1+"/"+name, content))
		test.AssertNoErr(api1.SyncAll())

		test.waitForNRefresh(workspaceName, 2)
		test.verifyContentStartsWith(file, content)
		file.Close()

		// Now delete the file in workspace1 and make sure it
		// is reflected in workspace0
		test.removeFile(workspace1, name)
		test.AssertNoErr(api1.SyncAll())
		test.waitForNRefresh(workspaceName, 3)
		test.assertNoFile(workspace0 + "/" + name)
	})
}

func TestRefreshDirReCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		ctx := test.TestCtx()
		dir1 := workspace + "/dir1"
		dir2 := workspace + "/dir2"
		filename := "testfile"
		content1 := "original content"
		content2 := "CONTENT2"

		test.AssertNoErr(syscall.Mkdir(dir1, 0777))
		CreateSmallFile(dir1+"/"+filename, content1)
		test.SyncWorkspace(test.RelPath(workspace))
		newRootId1 := test.getRootId(workspace)

		test.AssertNoErr(syscall.Mkdir(dir2, 0777))
		test.moveFile(workspace, "dir1/"+filename, "dir2/"+filename)
		test.AssertNoErr(testutils.OverWriteFile(
			dir2+"/"+filename, content2))
		test.AssertNoErr(syscall.Rmdir(dir1))
		test.AssertNoErr(syscall.Mkdir(dir1, 0777))

		file, err := os.OpenFile(dir2+"/"+filename, os.O_RDONLY, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content2)

		test.SyncWorkspace(test.RelPath(workspace))
		newRootId2 := test.getRootId(workspace)

		refreshTestNoRemount(ctx, test, workspace, newRootId2, newRootId1)

		test.verifyContentStartsWith(file, content1)
		test.AssertNoErr(file.Close())

		file, err = os.OpenFile(dir1+"/"+filename, os.O_RDONLY, 0777)
		test.AssertNoErr(err)
		test.verifyContentStartsWith(file, content1)
		test.removeFile(workspace, "dir1/"+filename)
		test.AssertNoErr(syscall.Rmdir(dir1))
		test.AssertNoErr(file.Close())
	})
}

func TestRefreshDualInstancesMultiDirty(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		c := test.TestCtx()
		mnt1 := test.qfsInstances[1].config.MountPath + "/"
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + workspaceName
		dir := "dir"
		file1 := "testFile1"
		file2 := "testFile2"
		content1 := "content"

		file2fullname0 := fmt.Sprintf("%s/%s/%s", workspace0, dir, file2)

		file1fullname1 := fmt.Sprintf("%s/%s/%s", workspace1, dir, file1)
		file2fullname1 := fmt.Sprintf("%s/%s/%s", workspace1, dir, file2)

		test.markImmutable(c, workspaceName)
		api1, err := quantumfs.NewApiWithPath(mnt1 + "api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		// Keep a handle to workspace0 to make sure it is refreshed
		wsr0, cleanup := test.GetWorkspaceRoot(workspace0)
		defer cleanup()
		test.Assert(wsr0 != nil, "workspace root does not exist")

		test.AssertNoErr(utils.MkdirAll(workspace1+"/"+dir, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1fullname1, content1))
		test.AssertNoErr(testutils.PrintToFile(file2fullname1, content1))

		test.waitForNRefresh(workspaceName, 1)

		test.WaitFor(file2fullname0+" to appear ", func() bool {
			file, err := os.OpenFile(file2fullname0, os.O_RDONLY, 0777)
			defer file.Close()
			return err == nil
		})
		file, err := os.OpenFile(file2fullname0, os.O_RDONLY, 0777)
		test.AssertNoErr(err)
		defer file.Close()
		test.verifyContentStartsWith(file, content1)
	})
}

func TestRefreshShortcutting(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/commondir/commonsubdir", 0777)
		test.createFile(workspace, "commondir/fileA", 1000)
		test.createFile(workspace, "commondir/fileB", 1000)
		test.createFile(workspace, "commondir/commonsubdir/fileC", 1000)
		ctx := test.TestCtx()

		name := "diffFile"
		newRootId1 := test.createFileSync(workspace, name, 1000)
		newRootId2 := test.removeFileSync(workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)
		test.Assert(test.CountLogStrings("buildRefreshMap") > 0,
			"Refresh never happened")
		test.Assert(test.CountLogStrings("buildRefreshMap "+workspace+
			"/commondir") == 0, "Common directory not skipped")
	})
}

func TestRefreshMultiDirtyHardlinks(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		c := test.TestCtx()
		mnt1 := test.qfsInstances[1].config.MountPath + "/"
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + workspaceName

		content1 := "original content"
		content2 := "CONTENT2"

		test.markImmutable(c, workspaceName)
		api1, err := quantumfs.NewApiWithPath(mnt1 + "api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		// Keep a handle to workspace0 to make sure it is refreshed
		wsr0, cleanup := test.GetWorkspaceRoot(workspace0)
		defer cleanup()
		test.Assert(wsr0 != nil, "workspace root does not exist")

		leg1 := workspace1 + "/dirA/leg1"
		leg2 := workspace1 + "/dirB/leg2"
		otherfilename := "/otherfile"
		var stat1, stat2 syscall.Stat_t

		test.AssertNoErr(utils.MkdirAll(workspace1+"/dirA", 0777))
		test.AssertNoErr(utils.MkdirAll(workspace1+"/dirB", 0777))
		test.AssertNoErr(CreateSmallFile(leg1, content1))
		test.AssertNoErr(syscall.Link(leg1, leg2))
		test.AssertNoErr(api1.SyncAll())

		test.AssertNoErr(CreateSmallFile(workspace1+otherfilename,
			"othercontent"))
		test.AssertNoErr(syscall.Stat(leg1, &stat1))
		test.AssertNoErr(CreateMediumFile(leg1, content2))

		test.WaitFor(otherfilename+" to appear ", func() bool {
			file, err := os.OpenFile(workspace0+otherfilename,
				os.O_RDONLY, 0777)
			defer file.Close()
			return err == nil
		})

		test.AssertNoErr(syscall.Stat(workspace0+"/dirA/leg1", &stat2))
		if stat1.Mtim.Nano() != stat2.Mtim.Nano() {
			// This might happen if the updates to the otherfile and the
			// hardlinkleg are coalesced for some reason.
			c.elog("updates are coalesced %d vs. %d",
				stat1.Mtim.Nano(), stat2.Mtim.Nano())
		}

		file, err := os.OpenFile(workspace0+"/dirA/leg1", os.O_RDONLY, 0777)
		test.AssertNoErr(err)
		defer file.Close()
		test.verifyContentStartsWith(file, content1)
	})
}
