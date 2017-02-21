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
import "time"
import "github.com/aristanetworks/quantumfs"

func TestHardlinkReload(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		// Create a couple files so we can copy its directory record
		data := genData(2000)
		testFileA := workspace + "/testFile"
		err := printToFile(testFileA, string(data[:1000]))
		test.assertNoErr(err)

		testFileB := workspace + "/testFileB"
		err = printToFile(testFileB, string(data))
		test.assertNoErr(err)

		// artificially insert some hardlinks into the map
		wsr := test.getWorkspaceRoot(workspace)

		files := wsr.children.records()
		for i := uint64(0); i < uint64(len(files)); i++ {
			record := files[i].(*quantumfs.DirectoryRecord)
			wsr.hardlinks[HardlinkId(i)] = newLinkEntry(record)
		}

		// Write another file to ensure the wsr is dirty
		testFileC := workspace + "/testFileC"
		err = printToFile(testFileC, string(data[:1000]))
		test.assertNoErr(err)

		// trigger a sync so the workspace is published
		test.syncAllWorkspaces()

		workspaceB := "branch/copyWorkspace/test"
		api := test.getApi()
		err = api.Branch(test.relPath(workspace), workspaceB)
		test.assert(err == nil, "Unable to branch")

		wsrB := test.getWorkspaceRoot(workspaceB)

		test.assert(len(wsr.hardlinks) == len(wsrB.hardlinks),
			"Hardlink map length not preserved: %v %v", wsr.hardlinks,
			wsrB.hardlinks)

		for k, l := range wsr.hardlinks {
			v := l.record
			linkBPtr, exists := wsrB.hardlinks[k]
			linkB := *(linkBPtr.record)
			test.assert(exists, "link not reloaded in new wsr")
			test.assert(v.Filename() == linkB.Filename(),
				"Filename not preserved")
			test.assert(v.Type() == linkB.Type(), "Type not preserved")
			test.assert(v.ID().String() == linkB.ID().String(),
				"ID not preserved")
			test.assert(v.Size() == linkB.Size(), "Size not preserved")
			test.assert(v.ModificationTime() == linkB.ModificationTime(),
				"Modtime not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.assert(v.Permissions() == linkB.Permissions(),
				"Permissions not preserved")
			test.assert(v.Owner() == linkB.Owner(),
				"OwnerID not preserved")
			test.assert(v.Group() == linkB.Group(),
				"GroupID not preserved")
			test.assert(v.ExtendedAttributes().String() ==
				linkB.ExtendedAttributes().String(),
				"ExtendedAttributes not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
		}
	})
}

func TestHardlinkRelay(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testData := genData(2000)

		file1 := workspace + "/orig_file"
		err := ioutil.WriteFile(file1, testData[:1000], 0777)
		test.assertNoErr(err)

		file2 := workspace + "/hardlink"
		err = syscall.Link(file1, file2)
		test.assertNoErr(err)

		file3 := workspace + "/second_file"
		err = ioutil.WriteFile(file3, testData[:577], 0777)
		test.assertNoErr(err)

		file4 := workspace + "/hardlink2"
		err = syscall.Link(file3, file4)
		test.assertNoErr(err)

		// Change file contents
		err = printToFile(file2, string(testData[1000:]))
		test.assertNoErr(err)

		// Change permissions
		err = os.Chmod(file2, 0654)
		test.assertNoErr(err)

		// Ensure that file1 changed
		readData, err := ioutil.ReadFile(file1)
		test.assert(bytes.Equal(readData, testData), "data not linked")

		info, err := os.Stat(file1)
		test.assertNoErr(err)
		test.assert(info.Mode().Perm() == 0654, "Permissions not linked")
		test.assert(info.Size() == int64(len(testData)), "Size not linked")

		infoLink, err := os.Stat(file2)
		test.assertNoErr(err)
		test.assert(info.ModTime() == infoLink.ModTime(),
			"hardlink instance modTimes not shared")

		// Ensure that file 3 and file4 didn't
		info2, err := os.Stat(file3)
		test.assertNoErr(err)
		test.assert(info.Mode().Perm() != info2.Mode().Perm(),
			"hardlink permissions not separate")
		test.assert(info.Size() != info2.Size(),
			"hardlink sizes not separate")
		test.assert(test.getInodeNum(file3) != test.getInodeNum(file1),
			"multiple different hardlinks joined")
		test.assert(info.ModTime() != info2.ModTime(),
			"hardlink mod times not separate")
	})
}

func TestHardlinkForget(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()
		config.DirtyFlushDelay = 100 * time.Millisecond
		test.startQuantumFs(config)

		workspace := test.newWorkspace()

		data := genData(2000)

		testFile := workspace + "/testFile"
		err := printToFile(testFile, string(data))
		test.assertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.assertNoErr(err)

		// Read the hardlink to ensure it's instantiated
		readData, err := ioutil.ReadFile(linkFile)
		test.assertNoErr(err)
		test.assert(bytes.Equal(data, readData), "hardlink data mismatch")

		// Forget it
		linkInode := test.getInodeNum(linkFile)

		remountFilesystem(test)

		// Check that it's uninstantiated
		msg := fmt.Sprintf("hardlink inode %d to be forgotten", linkInode)
		test.waitFor(msg, func() bool {
			inode := test.qfs.inodeNoInstantiate(&test.qfs.c, linkInode)
			return inode == nil
		})
	})
}

// When all hardlinks, but one, are deleted then we need to convert a hardlink back
// into a regular file.
func TestHardlinkConversion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		data := genData(2000)

		testFile := workspace + "/testFile"
		err := printToFile(testFile, string(data[:1000]))
		test.assertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.assertNoErr(err)

		linkInode := test.getInodeNum(linkFile)

		wsr := test.getWorkspaceRoot(workspace)
		linkId := func() HardlinkId {
			defer wsr.linkLock.Lock().Unlock()
			return wsr.inodeToLink[linkInode]
		}()

		err = os.Remove(testFile)
		test.assertNoErr(err)

		// Ensure it's converted by performing an operation on linkFile
		// that would trigger recordByName
		err = os.Rename(linkFile, linkFile+"_newname")
		test.assertNoErr(err)
		test.assertLogContains("ChildMap::recordByName",
			"recordByName not triggered by Rename")
		linkFile += "_newname"

		// ensure we can still use the file as normal
		err = printToFile(linkFile, string(data[1000:]))
		test.assertNoErr(err)

		output, err := ioutil.ReadFile(linkFile)
		test.assertNoErr(err)
		test.assert(bytes.Equal(output, data),
			"File not working after conversion from hardlink")

		wsr = test.getWorkspaceRoot(workspace)
		defer wsr.linkLock.Lock().Unlock()
		_, exists := wsr.hardlinks[linkId]
		test.assert(!exists, "hardlink not converted back to file")
	})
}

func TestHardlinkSubdirChain(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		data := genData(2000)

		err := os.Mkdir(workspace+"/dir", 0777)
		test.assertNoErr(err)

		testFile := workspace + "/dir/testFile"
		err = printToFile(testFile, string(data))
		test.assertNoErr(err)

		linkFile := workspace + "/dir/testLink"
		err = syscall.Link(testFile, linkFile)
		test.assertNoErr(err)

		linkFile2 := workspace + "/dir/testLink2"
		err = syscall.Link(linkFile, linkFile2)
		test.assertNoErr(err)

		linkFile3 := workspace + "/dir/testLink3"
		err = syscall.Link(linkFile2, linkFile3)
		test.assertNoErr(err)

		// Now link again from the original
		linkFile4 := workspace + "/dir/testLink4"
		err = syscall.Link(linkFile, linkFile4)
		test.assertNoErr(err)
	})
}

func TestHardlinkWsrChain(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		data := genData(2000)

		testFile := workspace + "/testFile"
		err := printToFile(testFile, string(data))
		test.assertNoErr(err)

		linkFile := workspace + "/testLink"
		err = syscall.Link(testFile, linkFile)
		test.assertNoErr(err)

		linkFile2 := workspace + "/testLink2"
		err = syscall.Link(linkFile, linkFile2)
		test.assertNoErr(err)

		linkFile3 := workspace + "/testLink3"
		err = syscall.Link(linkFile2, linkFile3)
		test.assertNoErr(err)

		// Now link again from the original
		linkFile4 := workspace + "/testLink4"
		err = syscall.Link(linkFile, linkFile4)
		test.assertNoErr(err)
	})
}

func TestHardlinkInterWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaceA := test.newWorkspace()
		workspaceB := test.newWorkspace()

		data := genData(1000)

		testFile := workspaceA + "/testFile"
		err := printToFile(testFile, string(data))
		test.assertNoErr(err)

		linkFileA := workspaceA + "/testLink"
		err = syscall.Link(testFile, linkFileA)
		test.assertNoErr(err)

		linkFail := workspaceB + "/testLinkFail"
		err = syscall.Link(linkFileA, linkFail)
		test.assert(err != nil,
			"qfs allows existing link copy to another wsr")
		test.assert(os.IsPermission(err),
			"qfs not returning EPERM for inter-wsr link")

		testFileB := workspaceA + "/testFileB"
		err = printToFile(testFileB, string(data))
		test.assertNoErr(err)

		linkFailB := workspaceB + "/testLinkFailB"
		err = syscall.Link(testFileB, linkFailB)
		test.assert(err != nil,
			"qfs allows creation of hardlink across workspace bounds")
		test.assert(os.IsPermission(err),
			"qfs not returning EPERM for link across wsrs")
	})
}

func TestHardlinkOpenUnlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		filename := workspace + "/file"
		linkname := workspace + "/link"

		file, err := os.Create(filename)
		test.assertNoErr(err)
		defer file.Close()

		file.WriteString("stuff")

		err = os.Link(filename, linkname)
		test.assertNoErr(err)

		err = os.Remove(filename)
		test.assertNoErr(err)

		err = os.Remove(linkname)
		test.assertNoErr(err)
	})
}

func matchXAttrHardlinkExtendedKey(path string, extendedKey []byte,
	test *testHelper, Type quantumfs.ObjectType) {

	key, type_, size, err := quantumfs.DecodeExtendedKey(string(extendedKey))
	test.assert(err == nil, "Error decompressing the packet")

	// Extract the internal ObjectKey from QuantumFS
	inode := test.getInode(path)
	parent := inode.parent(&test.qfs.c)
	// parent should be the workspace root
	wsr := parent.(*WorkspaceRoot)
	isHardlink, linkId := wsr.checkHardlink(inode.inodeNum())
	test.assert(isHardlink, "Expected hardlink isn't one.")

	valid, record := wsr.getHardlink(linkId)
	test.assert(valid, "Unable to get hardlink from wsr")

	// Verify the type and key matching
	test.assert(type_ == Type && size == record.Size() &&
		bytes.Equal(key.Value(), record.ID().Value()),
		"Error getting the key: %v with size of %d-%d, keys of %v-%v",
		err, Type, type_, key.Value(), record.ID().Value())
}

func TestHardlinkExtraction(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		filename := workspace + "/file"
		linkname := workspace + "/link"

		file, err := os.Create(filename)
		test.assertNoErr(err)
		file.WriteString("stuff")
		file.Close()

		err = os.Link(filename, linkname)
		test.assertNoErr(err)

		dst := make([]byte, quantumfs.ExtendedKeyLength)
		sz, err := syscall.Getxattr(filename, quantumfs.XAttrTypeKey, dst)
		test.assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the file key: %v with a size of %d",
			err, sz)

		matchXAttrHardlinkExtendedKey(filename, dst, test,
			quantumfs.ObjectTypeSmallFile)

		dst = make([]byte, quantumfs.ExtendedKeyLength)
		sz, err = syscall.Getxattr(filename, quantumfs.XAttrTypeKey, dst)
		test.assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the file key: %v with a size of %d",
			err, sz)

		matchXAttrHardlinkExtendedKey(linkname, dst, test,
			quantumfs.ObjectTypeSmallFile)
	})
}
