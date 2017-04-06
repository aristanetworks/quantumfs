// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that different parts of Hardlink support are working

import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

func TestHardlinkReload(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		err := utils.MkdirAll(workspace+"/subdir/grandchild", 0777)
		test.AssertNoErr(err)

		// Create a couple files so we can copy its directory record
		data := GenData(2000)
		testFileA := workspace + "/subdir/testFile"
		err = testutils.PrintToFile(testFileA, string(data[:1000]))
		test.AssertNoErr(err)

		testFileB := workspace + "/subdir/testFileB"
		err = testutils.PrintToFile(testFileB, string(data))
		test.AssertNoErr(err)

		// artificially insert some hardlinks into the map
		wsr := test.getWorkspaceRoot(workspace)

		err = syscall.Link(testFileA, workspace+"/subdir/linkFileA")
		test.AssertNoErr(err)
		err = syscall.Link(testFileA,
			workspace+"/subdir/grandchild/linkFileA2")
		test.AssertNoErr(err)
		err = syscall.Link(testFileB, workspace+"/linkFileB")
		test.AssertNoErr(err)

		// Write data to the hardlink to ensure it's syncChild function works
		err = testutils.PrintToFile(
			workspace+"/subdir/grandchild/linkFileA2",
			string(data[1000:]))
		test.AssertNoErr(err)

		var nstat syscall.Stat_t
		err = syscall.Stat(testFileA, &nstat)
		test.AssertNoErr(err)
		test.Assert(nstat.Nlink == 3,
			"Nlink incorrect: %d", nstat.Nlink)

		err = syscall.Stat(testFileB, &nstat)
		test.AssertNoErr(err)
		test.Assert(nstat.Nlink == 2,
			"Nlink incorrect: %d", nstat.Nlink)

		// Write another file to ensure the wsr is dirty
		testFileC := workspace + "/testFileC"
		err = testutils.PrintToFile(testFileC, string(data[:1000]))
		test.AssertNoErr(err)

		// trigger a sync so the workspace is published
		test.SyncAllWorkspaces()

		workspaceB := "branch/copyWorkspace/test"
		api := test.getApi()
		err = api.Branch(test.RelPath(workspace), workspaceB)
		test.Assert(err == nil, "Unable to branch")

		wsrB := test.getWorkspaceRoot(workspaceB)

		// ensure that the hardlink was able to sync
		wsrBFileA := test.absPath(workspaceB +
			"/subdir/grandchild/linkFileA2")
		readData, err := ioutil.ReadFile(wsrBFileA)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(readData, data),
			"Data not synced via hardlink")

		stat, err := os.Stat(wsrBFileA)
		test.AssertNoErr(err)
		test.Assert(stat.Size() == int64(len(data)), "file length mismatch")

		test.Assert(len(wsr.hardlinks) == len(wsrB.hardlinks),
			"Hardlink map length not preserved: %v %v", wsr.hardlinks,
			wsrB.hardlinks)

		for k, l := range wsr.hardlinks {
			linkBPtr, exists := wsrB.hardlinks[k]

			test.Assert(l.nlink == linkBPtr.nlink,
				"link reference count not preserved")

			linkB := *(linkBPtr.record)
			v := l.record
			test.Assert(exists, "link not reloaded in new wsr")
			test.Assert(v.Filename() == linkB.Filename(),
				"Filename not preserved")
			test.Assert(v.Type() == linkB.Type(), "Type not preserved")
			test.Assert(v.ID().String() == linkB.ID().String(),
				"ID not preserved")
			test.Assert(v.Size() == linkB.Size(), "Size not preserved")
			test.Assert(v.ModificationTime() == linkB.ModificationTime(),
				"Modtime not preserved")
			test.Assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.Assert(v.Permissions() == linkB.Permissions(),
				"Permissions not preserved")
			test.Assert(v.Owner() == linkB.Owner(),
				"OwnerID not preserved")
			test.Assert(v.Group() == linkB.Group(),
				"GroupID not preserved")
			test.Assert(v.ExtendedAttributes().String() ==
				linkB.ExtendedAttributes().String(),
				"ExtendedAttributes not preserved")
			test.Assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.Assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
		}
	})
}

func TestHardlinkRelay(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testData := GenData(2000)

		file1 := workspace + "/orig_file"
		err := ioutil.WriteFile(file1, testData[:1000], 0777)
		test.AssertNoErr(err)

		file2 := workspace + "/hardlink"
		err = syscall.Link(file1, file2)
		test.AssertNoErr(err)

		file3 := workspace + "/second_file"
		err = ioutil.WriteFile(file3, testData[:577], 0777)
		test.AssertNoErr(err)

		file4 := workspace + "/hardlink2"
		err = syscall.Link(file3, file4)
		test.AssertNoErr(err)

		// Change file contents
		err = testutils.PrintToFile(file2, string(testData[1000:]))
		test.AssertNoErr(err)

		// Change permissions
		err = os.Chmod(file2, 0654)
		test.AssertNoErr(err)

		// Ensure that file1 changed
		readData, err := ioutil.ReadFile(file1)
		test.Assert(bytes.Equal(readData, testData), "data not linked")

		info, err := os.Stat(file1)
		test.AssertNoErr(err)
		test.Assert(info.Mode().Perm() == 0654, "Permissions not linked")
		test.Assert(info.Size() == int64(len(testData)), "Size not linked")

		infoLink, err := os.Stat(file2)
		test.AssertNoErr(err)
		test.Assert(info.ModTime() == infoLink.ModTime(),
			"hardlink instance modTimes not shared")

		// Ensure that file 3 and file4 didn't
		info2, err := os.Stat(file3)
		test.AssertNoErr(err)
		test.Assert(info.Mode().Perm() != info2.Mode().Perm(),
			"hardlink permissions not separate")
		test.Assert(info.Size() != info2.Size(),
			"hardlink sizes not separate")
		test.Assert(test.getInodeNum(file3) != test.getInodeNum(file1),
			"multiple different hardlinks joined")
		test.Assert(info.ModTime() != info2.ModTime(),
			"hardlink mod times not separate")
	})
}

func TestHardlinkForget(t *testing.T) {
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		data := GenData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data))
		test.AssertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.AssertNoErr(err)

		// Read the hardlink to ensure it's instantiated
		readData, err := ioutil.ReadFile(linkFile)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(data, readData), "hardlink data mismatch")

		// Forget it
		linkInode := test.getInodeNum(linkFile)

		test.remountFilesystem()

		// Check that it's uninstantiated
		msg := fmt.Sprintf("hardlink inode %d to be uninstantiated",
			linkInode)
		test.WaitFor(msg, func() bool {
			inode := test.qfs.inodeNoInstantiate(&test.qfs.c, linkInode)
			return inode == nil
		})
	})
}

func TestHardlinkUninstantiateDirectory(t *testing.T) {
	// If a hardlink is a child of many directories, it shouldn't prevent those
	// directories from becoming uninstantiated simply because it itself is still
	// instantiated. It is likely being held open by some other directory or
	// handle.
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		data := GenData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data))
		test.AssertNoErr(err)

		dirName := workspace + "/dir"
		err = syscall.Mkdir(dirName, 0777)
		test.AssertNoErr(err)

		linkFile := dirName + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.AssertNoErr(err)

		// Read the hardlink to ensure it's instantiated
		readData, err := ioutil.ReadFile(linkFile)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(data, readData), "hardlink data mismatch")

		wsrInode := test.getInodeNum(workspace)
		dirInode := test.getInodeNum(dirName)
		linkInode := test.getInodeNum(linkFile)
		test.qfs.increaseLookupCount(linkInode)

		test.remountFilesystem()

		// Check that the directory parent uninstantiated, even if the
		// Hardlink itself cannot be.
		msg := fmt.Sprintf("hardlink parent inode %d to be uninstantiated",
			dirInode)
		test.WaitFor(msg, func() bool {
			inode := test.qfs.inodeNoInstantiate(&test.qfs.c, dirInode)
			return inode == nil
		})

		// Even though the directory "parent" should have been
		// uninstantiated, the WorkspaceRoot must not have been
		// uninstantiated because the hardlink is instantiated.
		msg = fmt.Sprintf("Not all children unloaded, %d in %d", linkInode,
			wsrInode)
		test.WaitFor("WSR to be held by instantiated hardlink",
			func() bool { return test.TestLogContains(msg) })

		test.qfs.shouldForget(linkInode, 1)
	})
}

// When all hardlinks, but one, are deleted then we need to convert a hardlink back
// into a regular file.
func TestHardlinkConversion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		data := GenData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data[:1000]))
		test.AssertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.AssertNoErr(err)

		linkInode := test.getInodeNum(linkFile)

		wsr := test.getWorkspaceRoot(workspace)
		linkId := func() HardlinkId {
			defer wsr.linkLock.Lock().Unlock()
			return wsr.inodeToLink[linkInode]
		}()

		err = os.Remove(testFile)
		test.AssertNoErr(err)

		// Ensure it's converted by performing an operation on linkFile
		// that would trigger checking if the hardlink needs conversion
		test.remountFilesystem()

		_, err = os.Stat(linkFile)
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()

		// ensure we can still use the file as normal
		err = testutils.PrintToFile(linkFile, string(data[1000:]))
		test.AssertNoErr(err)

		output, err := ioutil.ReadFile(linkFile)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(output, data),
			"File not working after conversion from hardlink")

		wsr = test.getWorkspaceRoot(workspace)
		defer wsr.linkLock.Lock().Unlock()
		_, exists := wsr.hardlinks[linkId]
		test.Assert(!exists, "hardlink not converted back to file")
	})
}

func TestHardlinkSubdirChain(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		data := GenData(2000)

		err := syscall.Mkdir(workspace+"/dir", 0777)
		test.AssertNoErr(err)

		testFile := workspace + "/dir/testFile"
		err = testutils.PrintToFile(testFile, string(data))
		test.AssertNoErr(err)

		linkFile := workspace + "/dir/testLink"
		err = syscall.Link(testFile, linkFile)
		test.AssertNoErr(err)

		linkFile2 := workspace + "/dir/testLink2"
		err = syscall.Link(linkFile, linkFile2)
		test.AssertNoErr(err)

		linkFile3 := workspace + "/dir/testLink3"
		err = syscall.Link(linkFile2, linkFile3)
		test.AssertNoErr(err)

		// Now link again from the original
		linkFile4 := workspace + "/dir/testLink4"
		err = syscall.Link(linkFile, linkFile4)
		test.AssertNoErr(err)
	})
}

func TestHardlinkWsrChain(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		data := GenData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data))
		test.AssertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.AssertNoErr(err)

		linkFile2 := workspace + "/testLink2"
		err = syscall.Link(linkFile, linkFile2)
		test.AssertNoErr(err)

		linkFile3 := workspace + "/testLink3"
		err = syscall.Link(linkFile2, linkFile3)
		test.AssertNoErr(err)

		// Now link again from the original
		linkFile4 := workspace + "/testLink4"
		err = syscall.Link(linkFile, linkFile4)
		test.AssertNoErr(err)
	})
}

func TestHardlinkInterWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaceA := test.NewWorkspace()
		workspaceB := test.NewWorkspace()

		data := GenData(1000)

		testFile := workspaceA + "/testFile"
		err := testutils.PrintToFile(testFile, string(data))
		test.AssertNoErr(err)

		linkFileA := workspaceA + "/testLink"
		err = syscall.Link(testFile, linkFileA)
		test.AssertNoErr(err)

		linkFail := workspaceB + "/testLinkFail"
		err = syscall.Link(linkFileA, linkFail)
		test.Assert(err != nil,
			"qfs allows existing link copy to another wsr")
		test.Assert(os.IsPermission(err),
			"qfs not returning EPERM for inter-wsr link")

		testFileB := workspaceA + "/testFileB"
		err = testutils.PrintToFile(testFileB, string(data))
		test.AssertNoErr(err)

		linkFailB := workspaceB + "/testLinkFailB"
		err = syscall.Link(testFileB, linkFailB)
		test.Assert(err != nil,
			"qfs allows creation of hardlink across workspace bounds")
		test.Assert(os.IsPermission(err),
			"qfs not returning EPERM for link across wsrs")
	})
}

func TestHardlinkOpenUnlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		filename := workspace + "/file"
		linkname := workspace + "/link"

		file, err := os.Create(filename)
		test.AssertNoErr(err)
		defer file.Close()

		file.WriteString("stuff")

		err = os.Link(filename, linkname)
		test.AssertNoErr(err)

		err = os.Remove(filename)
		test.AssertNoErr(err)

		err = os.Remove(linkname)
		test.AssertNoErr(err)
	})
}

func matchXAttrHardlinkExtendedKey(path string, extendedKey []byte,
	test *testHelper, Type quantumfs.ObjectType, wsr *WorkspaceRoot) {

	key, type_, size, err := quantumfs.DecodeExtendedKey(string(extendedKey))
	test.Assert(err == nil, "Error decompressing the packet")

	// Extract the internal ObjectKey from QuantumFS
	inode := test.getInode(path)
	// parent should be the workspace root.
	isHardlink, linkId := wsr.checkHardlink(inode.inodeNum())
	test.Assert(isHardlink, "Expected hardlink isn't one.")

	valid, record := wsr.getHardlink(linkId)
	test.Assert(valid, "Unable to get hardlink from wsr")

	// Verify the type and key matching
	test.Assert(type_ == Type && size == record.Size() &&
		bytes.Equal(key.Value(), record.ID().Value()),
		"Error getting the key: %v with size of %d-%d, keys of %v-%v",
		err, Type, type_, key.Value(), record.ID().Value())
}

func TestHardlinkExtraction(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		filename := workspace + "/file"
		linkname := workspace + "/link"

		file, err := os.Create(filename)
		test.AssertNoErr(err)
		file.WriteString("stuff")
		file.Close()

		err = os.Link(filename, linkname)
		test.AssertNoErr(err)

		dst := make([]byte, quantumfs.ExtendedKeyLength)
		sz, err := syscall.Getxattr(filename, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the file key: %v with a size of %d",
			err, sz)

		wsr := test.getWorkspaceRoot(workspace)
		matchXAttrHardlinkExtendedKey(filename, dst, test,
			quantumfs.ObjectTypeSmallFile, wsr)

		dst = make([]byte, quantumfs.ExtendedKeyLength)
		sz, err = syscall.Getxattr(filename, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the file key: %v with a size of %d",
			err, sz)

		matchXAttrHardlinkExtendedKey(linkname, dst, test,
			quantumfs.ObjectTypeSmallFile, wsr)
	})
}

func TestHardlinkRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		filename := workspace + "/file"
		linkname := workspace + "/link"

		files := make([]string, 0)

		data := GenData(2000)
		file, err := os.Create(filename)
		test.AssertNoErr(err)
		file.WriteString(string(data))
		file.Close()

		err = os.Link(filename, linkname)
		test.AssertNoErr(err)

		newLink := workspace + "/linkB"
		err = os.Rename(linkname, newLink)
		test.AssertNoErr(err)
		linkname = newLink

		err = syscall.Mkdir(workspace+"/dir", 0777)
		test.AssertNoErr(err)

		newLink = workspace + "/dir/linkC"
		err = os.Rename(linkname, newLink)
		test.AssertNoErr(err)
		linkname = newLink
		files = append(files, linkname)

		err = os.Link(filename, workspace+"/dir/linkE")
		test.AssertNoErr(err)
		files = append(files, workspace+"/dir/linkE")

		err = os.Rename(filename, workspace+"/linkD")
		test.AssertNoErr(err)
		files = append(files, workspace+"/linkD")

		for _, v := range files {
			readback, err := ioutil.ReadFile(v)
			test.AssertNoErr(err)
			test.Assert(bytes.Equal(readback, data),
				"file %s data not preserved", v)
		}
	})
}

func ManualLookup(c *ctx, parent Inode, childName string) {
	var dummy fuse.EntryOut
	defer parent.RLockTree().RUnlock()
	parent.Lookup(c, childName, &dummy)
}

func TestHardlinkReparentRace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		var stat syscall.Stat_t
		iterations := 50
		for i := 0; i < iterations; i++ {
			filename := fmt.Sprintf(workspace+"/file%d", i)
			linkname := fmt.Sprintf(workspace+"/link%d", i)
			file, err := os.Create(filename)
			test.AssertNoErr(err)

			err = syscall.Link(filename, linkname)
			test.AssertNoErr(err)

			file.WriteString("this is file data")
			file.Close()

			parent := test.getInode(workspace)

			// We want to race the parent change with getting the parent
			go os.Remove(filename)
			go ManualLookup(&test.qfs.c, parent, filename)
			go syscall.Stat(filename, &stat)
			go os.Remove(linkname)
		}
	})
}

func TestHardlinkUninstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		err := utils.MkdirAll(workspace+"/subdir/grandchild", 0777)
		test.AssertNoErr(err)

		filename := workspace + "/subdir/fileA"
		linkname := workspace + "/subdir/grandchild/fileB"
		data := GenData(2000)

		err = testutils.PrintToFile(filename, string(data))
		test.AssertNoErr(err)

		err = syscall.Link(filename, linkname)
		test.AssertNoErr(err)

		// trigger a sync so the workspace is published
		test.SyncAllWorkspaces()

		workspaceB := "branch/copyWorkspace/test"
		api := test.getApi()
		err = api.Branch(test.RelPath(workspace), workspaceB)
		test.AssertNoErr(err)

		readData, err := ioutil.ReadFile(test.absPath(workspaceB +
			"/subdir/grandchild/fileB"))
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(readData, data),
			"data mismatch after Branch")
	})
}

func (test *testHelper) LinkFileExp(path string, filename string) {
	err := utils.MkdirAll(path, 0777)
	test.AssertNoErr(err)

	// Enough data to consume a multi block file
	data := GenData(quantumfs.MaxBlockSize + 1000)

	filepath := path + "/" + filename
	linkpath := path + "/" + filename + "link"
	err = testutils.PrintToFile(filepath, string(data[:1000]))
	test.AssertNoErr(err)

	// Make them a link
	err = syscall.Link(filepath, linkpath)
	test.AssertNoErr(err)

	// Cause the underlying file to expand and change its own type
	err = testutils.PrintToFile(linkpath, string(data[1000:]))
	test.AssertNoErr(err)

	// Ensure that the file actually works
	readData, err := ioutil.ReadFile(linkpath)
	test.AssertNoErr(err)
	test.Assert(bytes.Equal(readData, data), "Link data wrong after expansion")
}

func TestHardlinkFileExpansionInWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		test.LinkFileExp(workspace, "fileA")
	})
}

func TestHardlinkFileExpansionOutWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		test.LinkFileExp(workspace+"/dirB", "fileB")
	})
}

// Once a hardlink record is returned to a class for use, the hardlink may be
// unlinked before the record is used. We need to accommodate that.
func TestHardlinkRecordRace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		data := GenData(100)

		// This is a race condition, so repeat to increase the likelihood
		for i := 0; i < 100; i++ {
			filename := fmt.Sprintf("%s/file%d", workspace, i)
			err := testutils.PrintToFile(filename, string(data))
			test.AssertNoErr(err)

			err = syscall.Link(filename, filename+"link")
			test.AssertNoErr(err)

			for i := 0; i < 10; i++ {
				go os.Stat(filename)
			}
			// quickly remove the link before all of the GetAttrs finish
			errA := os.Remove(filename)
			errB := os.Remove(filename + "link")
			test.AssertNoErr(errA)
			test.AssertNoErr(errB)
		}
	})
}

func TestHardlinkDeleteFromDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		dir1 := workspace + "/dir1/dir1.1"
		err := utils.MkdirAll(dir1, 0777)
		test.AssertNoErr(err)

		dir2 := workspace + "/dir2"
		err = utils.MkdirAll(dir2, 0777)
		test.AssertNoErr(err)

		filename := dir1 + "/fileA"
		linkname := dir2 + "/link"
		data := GenData(2000)

		err = testutils.PrintToFile(filename, string(data))
		test.AssertNoErr(err)

		err = syscall.Link(filename, linkname)
		test.AssertNoErr(err)

		err = os.RemoveAll(dir1)
		test.AssertNoErr(err)
	})
}
