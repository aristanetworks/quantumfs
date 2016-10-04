// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that workspaceroot maintains a list of accessed files
import "fmt"
import "os"
import "syscall"
import "reflect"
import "testing"

func TestFileCreateAccessList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error")
		accessList[filename] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		path = test.absPath(bworkspace) + filename
		file, err := os.Open(path)
		test.assert(err == nil, "Open file error")
		accessList[filename] = false
		file.Close()

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)
	})
}

func TestFileCreateDeleteList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error")
		accessList[filename] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		path = test.absPath(bworkspace) + filename
		err = os.Remove(path)
		test.assert(err == nil, "Open file error")
		accessList[filename] = false

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestDirectoryCreateDeleteList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error")
		accessList[dirname] = true

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		path = test.absPath(bworkspace) + dirname
		err = syscall.Rmdir(path)
		test.assert(err == nil, "Delete directory error")
		accessList[dirname] = false

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestRecursiveCreateRemoveList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		filename := "/test.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		path = workspace + dirname + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname+filename] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		path = test.absPath(bworkspace) + dirname + filename
		err = os.Remove(path)
		test.assert(err == nil, "Delete file error")
		accessList[dirname+filename] = false
		path = test.absPath(bworkspace) + dirname
		err = syscall.Rmdir(path)
		test.assert(err == nil, "Delete directory error")
		accessList[dirname] = false

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestMvChildList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname1 := "/test1"
		dirname2 := "/test2"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		filename3 := "/test3.c"
		path := workspace + dirname1
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname1] = true
		path = workspace + dirname1 + filename1
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname1+filename1] = true
		syscall.Close(fd)

		path = workspace + dirname2
		err = syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname2] = true
		path = workspace + dirname2 + filename2
		fd, err = syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname2+filename2] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		accessList = make(map[string]bool)

		path1 := test.absPath(bworkspace) + dirname1 + filename1
		path2 := test.absPath(bworkspace) + dirname2 + filename3
		err = os.Rename(path1, path2)
		test.assert(err == nil, "Move file error")
		accessList[dirname1] = false
		accessList[dirname2] = false
		accessList[dirname1+filename1] = false
		accessList[dirname2+filename3] = true

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestRenameChildList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname+filename1] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		accessList = make(map[string]bool)

		path1 := test.absPath(bworkspace) + dirname + filename1
		path2 := test.absPath(bworkspace) + dirname + filename2
		err = os.Rename(path1, path2)
		test.assert(err == nil, "Move file error")
		accessList[dirname] = false
		accessList[dirname+filename1] = false
		accessList[dirname+filename2] = true

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestHardLinkList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname+filename1] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		accessList = make(map[string]bool)

		path1 := test.absPath(bworkspace) + dirname + filename1
		path2 := test.absPath(bworkspace) + dirname + filename2
		err = syscall.Link(path1, path2)
		test.assert(err == nil, "Create hard link error")
		accessList[dirname] = false
		accessList[dirname+filename1] = false
		accessList[dirname+filename2] = true

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestSymlinkList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname+filename1] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		accessList = make(map[string]bool)

		path1 := test.absPath(bworkspace) + dirname + filename1
		path2 := test.absPath(bworkspace) + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.assert(err == nil, "Create symlink error")
		accessList[dirname] = false
		accessList[dirname+filename2] = true

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestSpecialFilesList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)

		path := workspace + "/test1"
		err := syscall.Mknod(path, syscall.S_IFBLK|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Make special file error")
		accessList["/test1"] = true

		path = workspace + "/test2"
		err = syscall.Mknod(path, syscall.S_IFCHR|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Make special file error")

		accessList["/test2"] = true

		path = workspace + "/test3"
		err = syscall.Mknod(path, syscall.S_IFSOCK|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Make special file error")

		accessList["/test3"] = true

		path = workspace + "/test4"
		err = syscall.Mknod(path, syscall.S_IFREG|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Make special file error")

		accessList["/test4"] = true

		wsr, ok := test.qfs.activeWorkspaces[relworkspace]
		test.assert(ok,
			"WorkspaceRoot "+relworkspace+" doesn't exist")

		eq := reflect.DeepEqual(accessList, wsr.accessList)
		msg := fmt.Sprintf("testlist:%v wsrlist: %v",
			accessList, wsr.accessList)
		test.assert(eq,
			"Error two maps not equal, map content:"+msg)

	})
}

func TestReadSymlinkList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		path1 := workspace + dirname + filename1
		fd, err := syscall.Creat(path1, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[dirname+filename1] = true
		syscall.Close(fd)
		path2 := workspace + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.assert(err == nil, "Create symlink error:%v", err)
		accessList[dirname+filename2] = true

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		accessList = make(map[string]bool)

		path2 = test.absPath(bworkspace) + dirname + filename2
		path1, err = os.Readlink(path2)
		test.assert(err == nil, "Read symlink error:%v", err)
		accessList[dirname] = false
		accessList[dirname+filename2] = false

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}

func TestOverwriteRemovedList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		relworkspace := test.relPath(workspace)
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[filename] = true
		syscall.Close(fd)

		wsrlist := test.getAccessList(relworkspace)
		test.compareAccessList(accessList, wsrlist)

		bworkspace := test.branchWorkspace(workspace)
		path = test.absPath(bworkspace) + filename
		err = os.Remove(path)
		test.assert(err == nil, "Remove file error:%v", err)
		fd, err = syscall.Creat(path, 0666)
		accessList[filename] = true
		syscall.Close(fd)

		wsrlist = test.getAccessList(bworkspace)
		test.compareAccessList(accessList, wsrlist)

	})
}
