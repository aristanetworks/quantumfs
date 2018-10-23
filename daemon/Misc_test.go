// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test cases which do not belong in other test files

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/hanwen/go-fuse/fuse"
)

func TestUnknownInodeId(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		// We need to create enough files that we can read
		// some from the directory without reading the entire
		// directory. Then we can cause a directory snapshot
		// to be taken, delete the file filename and then
		// continue reading. This will result in the inodeId
		// for the filename being returned to the kernel after
		// that ID is no longer valid. This entry will be
		// cached in the kernel and the subsequent open call
		// will cause an inode number entirely unknown to
		// QuantumFS to be used in QuantumFs.Open().
		for i := 0; i < 300; i++ {
			file := fmt.Sprintf("%s/filler-%d", workspace, i)
			test.AssertNoErr(testutils.PrintToFile(file, "contents"))
		}
		test.AssertNoErr(testutils.PrintToFile(filename, "contents"))
		inodeNum := test.getInodeNum(filename)

		dir, err := os.Open(workspace)
		test.AssertNoErr(err)
		defer dir.Close()
		_, err = dir.Readdir(10)
		test.AssertNoErr(err)

		test.AssertNoErr(syscall.Unlink(filename))

		test.SyncAllWorkspaces()
		test.qfs.Forget(uint64(inodeNum), 1)

		_, err = dir.Readdir(0)
		test.AssertNoErr(err)

		_, err = os.Open(filename)
		test.AssertErr(err)
		test.Assert(os.IsNotExist(err),
			"Expected ENOENT, got %s", err.Error())
	})
}

func TestDualInstances(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		expectedData := test.MakeFile(filename)
		test.SyncAllWorkspaces()

		path := test.qfsInstances[1].config.MountPath + "/" +
			test.RelPath(filename)

		test.CheckData(path, expectedData)
	})
}

func TestWorkspacePubSubCallback(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		test.MakeFile(filename)
		test.SyncAllWorkspaces()

		test.WaitForLogString("Mux::handleWorkspaceChanges",
			"Workspace pubsub callback to be called")
	})
}

func newInodeId(c *ctx, ids *inodeIds) InodeId {
	newId, _ := ids.newInodeId(c)
	return newId
}

func TestInodeIdsIncrementing(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		ids := newInodeIds(100*time.Millisecond, time.Hour)
		c := test.newCtx()
		test.Assert(newInodeId(c, ids) == 4, "Wrong 1st inodeId given")
		test.Assert(newInodeId(c, ids) == 5, "Wrong 2nd inodeId given")
		test.Assert(newInodeId(c, ids) == 6, "Wrong 3rd inodeId given")

		ids.releaseInodeId(c, 4)
		time.Sleep(50 * time.Millisecond)
		test.Assert(newInodeId(c, ids) == 7, "Wrong next id during delay")
		time.Sleep(60 * time.Millisecond)

		test.Assert(newInodeId(c, ids) == 4, "Didn't get to reuse 1st id")
		test.Assert(newInodeId(c, ids) == 8, "Wrong next id")
	})
}

func TestInodeIdsGarbageCollection(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		ids := newInodeIds(time.Millisecond, 100*time.Millisecond)
		c := test.newCtx()

		allocated := make([]InodeId, 100, 100)
		for i := 0; i < 100; i++ {
			allocated[i] = newInodeId(c, ids)
		}

		for i := 0; i < 100; i++ {
			ids.releaseInodeId(c, allocated[i])
		}

		time.Sleep(time.Millisecond * 10)

		func() {
			defer ids.lock.Lock().Unlock()
			test.Assert(ids.highMark == 104,
				"Garbage collection happened too soon")
		}()

		time.Sleep(time.Millisecond * 100)

		// go through all the ids and ensure that we garbage collected
		for i := 0; i < 90; i++ {
			newInodeId(c, ids)
		}

		func() {
			defer ids.lock.Lock().Unlock()
			test.Assert(ids.highMark == 94,
				"Garbage collection happened too soon")
		}()

		test.Assert(newInodeId(c, ids) == 94,
			"inodeIds didn't resume counting after GC")
	})
}

func TestInodeIdsReuseCheck(t *testing.T) {
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		func() {
			defer test.qfs.inodeIds.lock.Lock().Unlock()
			test.qfs.inodeIds.reusableDelay = time.Millisecond * 2
			test.qfs.inodeIds.gcPeriod = time.Millisecond * 20
		}()

		test.AssertNoErr(os.MkdirAll(workspace+"/dirA/dirB", 0777))
		test.MakeFile(workspace + "/dirA/dirB/fileA")

		test.AssertNoErr(os.MkdirAll(workspace+"/dirA/dirC", 0777))
		test.MakeFile(workspace + "/dirA/dirC/fileB")

		fileA := test.getInodeNum(workspace + "/dirA/dirB/fileA")
		dirC := test.getInodeNum(workspace + "/dirA/dirC")
		fileB := test.getInodeNum(workspace + "/dirA/dirC/fileB")
		test.Assert(dirC == fileA+1, "inode id not simply incremented")
		test.Assert(fileB == dirC+1, "inode id not simply incremented")

		c := test.newCtx()
		// wait for garbage collection to happen at least once
		test.WaitFor("inode ids to be garbage collected", func() bool {
			defer test.qfs.inodeIds.lock.Lock().Unlock()
			test.qfs.inodeIds.testHighmark_(c)
			return test.qfs.inodeIds.highMark < uint64(fileB)
		})

		test.MakeFile(workspace + "/dirA/fileC")
		test.MakeFile(workspace + "/dirA/fileD")
		fileC := test.getInodeNum(workspace + "/dirA/fileC")
		fileD := test.getInodeNum(workspace + "/dirA/fileD")
		test.Assert(fileC == fileB+1, "inode id not incremented after GC")
		test.Assert(fileD == fileC+1, "inode id not incremented after GC")
	})
}

func TestInodeIdsReuseIdsRecycled(t *testing.T) {
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		func() {
			defer test.qfs.inodeIds.lock.Lock().Unlock()
			test.qfs.inodeIds.reusableDelay = time.Millisecond * 2
			// We want inode id GC to happen after we've flushed so
			// that when the flusher releases the ids they go into the
			// reuse queue instead of being gc'ed
			test.qfs.inodeIds.gcPeriod = time.Millisecond * 150
		}()

		test.AssertNoErr(os.MkdirAll(workspace+"/dirA/dirB", 0777))
		test.MakeFile(workspace + "/dirA/dirB/fileA")
		test.MakeFile(workspace + "/dirA/dirB/fileB")

		fileA := test.getInodeNum(workspace + "/dirA/dirB/fileA")
		fileB := test.getInodeNum(workspace + "/dirA/dirB/fileB")
		dirB := test.getInodeNum(workspace + "/dirA/dirB")
		test.Assert(fileA == dirB+1, "inode id not simply incremented")
		test.Assert(fileB == fileA+1, "inode id not simply incremented")

		// Artificially trigger Forget early
		test.ForceForget(dirB)

		// Give the flusher time
		time.Sleep(150 * time.Millisecond)

		c := test.newCtx()
		// wait for garbage collection to happen
		test.WaitFor("inode ids to be garbage collected", func() bool {
			defer test.qfs.inodeIds.lock.Lock().Unlock()
			test.qfs.inodeIds.testHighmark_(c)
			// Wait for all three inode ids to be ready for reuse
			return test.qfs.inodeIds.highMark < uint64(fileA)
		})

		idsReseen := 0
		newIds := make(map[InodeId]struct{})
		for i := 0; i < 10; i++ {
			file := fmt.Sprintf("file%d", i)
			test.MakeFile(workspace + "/" + file)
			inodeId := test.getInodeNum(workspace + "/" + file)

			_, exists := newIds[inodeId]
			test.Assert(!exists, "Duplicate inode id given %d", inodeId)
			newIds[inodeId] = struct{}{}

			if inodeId == fileA || inodeId == fileB {
				idsReseen++
			}
		}

		test.Assert(idsReseen == 2, "Didn't see all inode ids reused: %d",
			idsReseen)
	})
}

func TestInstantiationPanicRecovery(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ExpectedErrors = make(map[string]struct{})
		test.ExpectedErrors["ERROR: Couldn't get from any store: "+
			"%s Key %s"] = struct{}{}
		test.ExpectedErrors["ERROR: PANIC (%d): 'No baseLayer object' "+
			"BT: %v"] = struct{}{}

		backingStore := newTestDataStore(test)
		test.SetDataStore(backingStore)
		// Make sure we cache nothing
		test.qfs.c.dataStore.cache = newCombiningCache(0)

		workspace := test.NewWorkspace()
		api := test.getApi()
		test.AssertNoErr(os.MkdirAll(workspace+"/dir", 0777))
		test.MakeFile(workspace + "/dir/file")

		test.SyncAllWorkspaces()

		// Delete a block artificially from the datastore to cause a panic
		// during loadAllChildren
		key := make([]byte, quantumfs.ExtendedKeyLength)
		_, err := syscall.Getxattr(workspace+"/dir",
			quantumfs.XAttrTypeKey, key)
		test.AssertNoErr(err)
		func() {
			defer backingStore.holeLock.Lock().Unlock()
			binaryKey, _, _, err := quantumfs.DecodeExtendedKey("" +
				string(key))
			test.AssertNoErr(err)
			backingStore.holes[binaryKey.String()] = struct{}{}
		}()

		test.AssertNoErr(api.Branch(test.RelPath(workspace),
			"test/test/branchB"))

		ioutil.ReadFile(test.AbsPath("test/test/branchB/dir/file"))
	})
}

func (test *testHelper) getGenerationNumber(dir string, file string) uint64 {
	inodeNum := test.getInodeNum(dir)
	test.Assert(inodeNum != quantumfs.InodeIdInvalid, "Invalid inode")

	var header fuse.InHeader
	header.NodeId = uint64(inodeNum)
	var out fuse.EntryOut
	status := test.qfs.Lookup(&header, file, &out)
	test.Assert(status == fuse.OK, "Lookup on %s/%s failed", dir, file)

	return out.Generation
}

func TestInodeIdsReuseGeneration(t *testing.T) {
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		func() {
			defer test.qfs.inodeIds.lock.Lock().Unlock()
			test.qfs.inodeIds.reusableDelay = time.Millisecond * 2
			test.qfs.inodeIds.gcPeriod = time.Millisecond * 2000
		}()

		api := test.getApi()

		test.AssertNoErr(os.MkdirAll(workspace+"/dirA", 0777))
		test.AssertNoErr(testutils.PrintToFile(workspace+"/dirA/fileA",
			"some data"))

		// Branch into a new workspace so that nothing is instantiated
		test.AssertNoErr(api.Branch(test.RelPath(workspace),
			"test/test/test"))
		test.AssertNoErr(api.EnableRootWrite("test/test/test"))
		workspace = test.AbsPath("test/test/test")

		dirA := test.getInodeNum(workspace + "/dirA")
		fileAGen := test.getGenerationNumber(workspace+"/dirA", "fileA")
		fileA := test.getInodeNum(workspace + "/dirA/fileA")

		// Trigger Forget early
		test.qfs.Forget(uint64(fileA), 2)
		test.qfs.Forget(uint64(dirA), 1)

		// Give the flusher time
		time.Sleep(150 * time.Millisecond)

		// We expect the first inode we make to be the reused fileA inode
		test.AssertNoErr(testutils.PrintToFile(workspace+"/newFile",
			"some data"))

		newInodeId := test.getInodeNum(workspace + "/newFile")
		test.Assert(newInodeId == fileA, "inode id not reused %d %d",
			newInodeId, fileA)

		newInodeGen := test.getGenerationNumber(workspace, "newFile")
		test.Assert(newInodeGen != fileAGen,
			"generation number not different %d %d", newInodeGen,
			fileAGen)
	})
}

func stacksMatch(a []lockInfo, b []lockInfo) bool {
	if len(a) != len(b) {
		return false
	}

	for idx, info := range a {
		other := b[idx]
		if info.kind != other.kind || info.inode != other.inode {
			return false
		}
	}

	return true
}

func TestLockCheckStack(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		test.AssertNoErr(os.MkdirAll(workspace+"/dirA/dirB", 0777))
		fileA := workspace + "/dirA/dirB/fileA"
		fileB := workspace + "/dirA/dirB/fileB"
		test.AssertNoErr(testutils.PrintToFile(fileA, "some data"))
		test.AssertNoErr(syscall.Link(fileA, fileB))

		test.SyncAllWorkspaces()

		// We're targeting the setChildAttr path for hardlinks, via
		// reconcileFileType, to get as much stack as we can
		wsr := test.getInode(workspace).(*WorkspaceRoot)
		test.Assert(wsr != nil, "Unable to get workspaceroot")
		fileAId := test.getInodeNum(fileA)
		// parent is workspaceroot since they're hardlinks
		dirId := test.getInodeNum(workspace)

		linkUnlock := callOnce(wsr.hardlinkTable.linkLock.Lock().Unlock)
		defer linkUnlock.invoke()

		// in parallel now, trigger the lock going up
		c := test.qfs.c.newThread()
		c.fuseCtx = &fuse.Context{}
		go func() {
			inode, unlock := test.qfs.RLockTreeGetInode(c, fileAId)
			defer unlock()

			var input fuse.SetAttrIn
			input.Valid |= fuse.FATTR_SIZE
			input.Size = quantumfs.MaxSmallFileSize() + 100
			var out fuse.AttrOut
			inode.SetAttr(c, &input, &out)
		}()

		// wait for our other thread to block
		test.WaitForLogString(fmt.Sprintf("modifyChildWithFunc inode %d",
			fileAId), "Parallel thread to iterate up")

		// copy the stack
		stack := make([]lockInfo, len(c.lockOrder.stack))
		copy(stack, c.lockOrder.stack)
		linkUnlock.invoke()

		compare := make([]lockInfo, 4, 4)
		compare[0] = newLockInfoQuick(lockerParentLock, fileAId)
		compare[1] = newLockInfoQuick(lockerInodeLock, fileAId)
		compare[2] = newLockInfoQuick(lockerInodeLock, dirId)
		compare[3] = newLockInfoQuick(lockerChildRecordLock, dirId)
		test.Assert(stacksMatch(stack, compare), "stacks mismatch %v %v",
			stack, compare)
	})
}

func TestLockCheckInvertedStack(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ExpectedErrors = make(map[string]struct{})
		test.ExpectedErrors["ERROR: "+lockInversionLog] = struct{}{}
		test.ExpectedErrors["ERROR: Stack: %s\n%s"] = struct{}{}

		workspace := test.NewWorkspace()
		test.AssertNoErr(testutils.PrintToFile(workspace+"/file",
			"some data"))

		test.SyncAllWorkspaces()

		fileParent := test.getInode(workspace)

		c := test.qfs.c.newThread()
		c.fuseCtx = &fuse.Context{}

		defer fileParent.Lock(c).Unlock()

		go func() {
			inode, unlock := test.qfs.RLockTreeGetInode(c,
				fileParent.inodeNum())
			defer unlock()

			var input fuse.CreateIn
			var out fuse.CreateOut
			inode.Create(c, &input, "newFile", &out)
		}()

		test.WaitForLogString("Lock inversion detected", "Locks to invert")
	})
}
