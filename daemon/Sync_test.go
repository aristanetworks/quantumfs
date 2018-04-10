// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test Inode syncing to ensure it happens correctly and only happens when it
// supposed to.

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

// This test dataStore counts the number of sets, useful for determining when
// quantumfs is syncing
type setCountingDataStore struct {
	quantumfs.DataStore
	setCount uint64
}

func (store *setCountingDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buffer quantumfs.Buffer) error {

	c.Dlog(qlog.LogDatastore, "setCountingDataStore::Set()")
	atomic.AddUint64(&store.setCount, 1)
	return store.DataStore.Set(c, key, buffer)
}

func TestSyncFileOverwrite(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()

		// Make an instance of QuantumFs and put things there
		test.startQuantumFs(config, nil, false)
		workspace := test.NewWorkspace()

		// Generate some deterministic, pseudorandom data for a folder
		// structure, treating each number as a command
		data := GenData(500)
		totalWritten := 0
		dataWidth := 50
		for i := 0; i < len(data)/dataWidth; i++ {
			fd, err := os.OpenFile(workspace+"/testFile",
				os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			test.Assert(err == nil, "Unable to open testFile %s", err)

			toWrite := data[i*dataWidth : (i+1)*dataWidth]
			bufferedWriter := bufio.NewWriter(fd)
			wrote, err := bufferedWriter.Write(toWrite)
			totalWritten += wrote
			test.Assert(err == nil, "Unable to write to testFile")

			bufferedWriter.Flush()
			fd.Close()

			test.SyncAllWorkspaces()
		}
		test.Assert(totalWritten == len(data), "Written mismatch")

		test.putApi()
		err := test.qfs.server.Unmount()
		test.Assert(err == nil, "Failed to unmount during test")

		test.startQuantumFs(config, nil, false)

		fileRestored, err := ioutil.ReadFile(workspace + "/testFile")
		test.Assert(err == nil, "Unable to read file after qfs reload: %s",
			err)
		test.Assert(bytes.Equal(data, fileRestored),
			"File contents not completely synced / restored %d %d",
			len(data), len(fileRestored))
	})
}

func TestSyncToDatastore(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()

		// Make an instance of QuantumFs and put things there
		test.startQuantumFs(config, nil, false)
		workspace := test.NewWorkspace()

		// Generate some deterministic, pseudorandom data for a folder
		// structure, treating each number as a command. Ensure GenData
		// generates a long enough string of natural numbers to ensure at
		// least a few sync commands occur.
		data := GenData(50)
		folderStack := make([]string, 0)
		for i := 0; i < len(data); i++ {
			// and occasionally force a sync
			if data[i] == '0' {
				test.SyncAllWorkspaces()
			}

			if data[i] < '3' {
				folderStack = append(folderStack, "/folder")
				folderStr := strings.Join(folderStack, "")
				utils.MkdirAll(workspace+folderStr, 0777)
			} else if data[i] < '5' && len(folderStack) > 0 {
				folderStack = folderStack[:len(folderStack)-1]
			} else {
				folderStr := strings.Join(folderStack, "")
				ioutil.WriteFile(workspace+folderStr+"/file"+
					strconv.Itoa(i), []byte(data), os.ModePerm)
			}
		}

		// Sync all of the data we've written
		test.SyncAllWorkspaces()

		// Now end quantumfs A, and start B with the same datastore so we
		// can verify that the data was preserved via the datastore
		test.putApi()
		err := test.qfs.server.Unmount()
		test.Assert(err == nil, "Failed to unmount during test")
		test.waitForQuantumFsToFinish()

		test.startQuantumFs(config, nil, false)

		// Iterate in the exact same way, but this time verifying instead of
		// creating the data
		folderStack = make([]string, 0)
		for i := 0; i < len(data); i++ {
			if data[i] < '3' {
				folderStack = append(folderStack, "/folder")
				folderStr := strings.Join(folderStack, "")

				_, err := os.Stat(workspace + folderStr)
				test.Assert(err == nil, "Folder lost (%s): %v",
					workspace+folderStr, err)
			} else if data[i] < '5' && len(folderStack) > 0 {
				folderStack = folderStack[:len(folderStack)-1]
			} else {
				folderStr := strings.Join(folderStack, "")
				fileName := workspace + folderStr + "/file" +
					strconv.Itoa(i)

				fileData, err := ioutil.ReadFile(fileName)

				test.Assert(err == nil, "File lost (%s): %s",
					fileName, err)
				test.Assert(bytes.Equal(fileData, data),
					"File data doesn't match in %s", fileName)
			}
		}
	})
}

// Put in place a proxy dataStore which counts the stores we make, then create a few
// directories and file and put some data into those files. During all this we do not
// expect any writes to be made into the datastore. Finally sync all the workspaces
// and confirm some writes happened.
func TestNoImplicitSync(t *testing.T) {
	runTest(t, func(test *testHelper) {
		dataStore := setCountingDataStore{
			DataStore: test.qfs.c.dataStore.durableStore,
		}
		test.qfs.c.dataStore.durableStore = &dataStore

		workspace := test.NewWorkspace()

		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/c"

		// Create a directory
		err := utils.MkdirAll(dirName, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating file: %v", err)
		defer file.Close()

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.Assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IRUSR | syscall.S_IWUSR
		expectedPermissions |= syscall.S_IRGRP | syscall.S_IWGRP
		expectedPermissions |= syscall.S_IROTH | syscall.S_IWOTH
		test.Assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		// Write a small file
		origData := []byte("test text")
		_, err = file.Write(origData)
		test.Assert(err == nil, "Error writing to file %v", err)

		// Confirm nothing to this point has synced anything
		setCount := atomic.LoadUint64(&dataStore.setCount)
		test.Assert(setCount == 0, "Datastore sets occurred! %d", setCount)

		// Confirm the data is correct
		data, err := ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading from file %v", err)
		test.Assert(bytes.Equal(data, origData), "Data does not match %v",
			data)
		// ioutil.ReadFile() opens and closes the file. This causes a
		// Flush() which will sync the file. Record the number of datastore
		// writes to compare against later.
		expectedCount := atomic.LoadUint64(&dataStore.setCount)

		// Write a medium file
		err = file.Truncate(20 * 1024 * 1024)
		test.Assert(err == nil, "Error extending small file %v", err)

		// Confirm the only sync at this point was the move from small file
		// to medium file.
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.Assert(setCount == expectedCount, "Unexpected store writes %d",
			setCount)

		data, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Error reading from file %v", err)
		test.Assert(bytes.Equal(data[:len(origData)], origData),
			"Data does not match %v", data)
		expectedCount = atomic.LoadUint64(&dataStore.setCount)

		// Extend the file even further
		origData = []byte("more text")
		_, err = file.WriteAt(origData, 21*1024*1024)
		test.Assert(err == nil, "Error writing deep into medium file %v",
			err)
		buf := make([]byte, 100)
		read, err := file.ReadAt(buf, 21*1024*1024)
		test.Assert(read == len(origData),
			"Read unexpected number of bytes %d != %d", read,
			len(origData))
		test.Assert(err == io.EOF, "Expected end of file! %v", err)
		test.Assert(bytes.Equal(buf[:read], origData),
			"Incorrect data read back %v", buf)

		// Now we sync everything and confirm that writes to the datastore
		// happen
		test.SyncAllWorkspaces()
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.Assert(setCount > expectedCount,
			"Datastore sets didn't happen! %d", setCount)
	})
}

func (test *testHelper) workspaceWaitChan(workspaceName string) chan struct{} {
	c := make(chan struct{})
	wsdb := test.qfsInstances[0].config.WorkspaceDB
	wsdbPl := wsdb.(*processlocal.WorkspaceDB)
	wsdb = wsdbPl.GetAdditionalHead()
	callback := func(updates map[string]quantumfs.WorkspaceState) {
		c <- struct{}{}
	}
	wsdb.SetCallback(callback)
	wsdb.SubscribeTo(workspaceName)

	return c
}

func TestPublishRecordsToBeConsistent(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		c := test.TestCtx()
		mnt1 := test.qfsInstances[1].config.MountPath + "/"
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + workspaceName
		content0 := "stale"
		content1 := "content"

		file2mnt0 := workspace0 + "/dir/testFile2"

		file1mnt1 := workspace1 + "/dir/testFile1"
		file2mnt1 := workspace1 + "/dir/testFile2"

		test.markImmutable(c, workspaceName)
		api1, err := quantumfs.NewApiWithPath(mnt1 + "api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		test.AssertNoErr(utils.MkdirAll(workspace1+"/dir", 0777))
		test.AssertNoErr(testutils.PrintToFile(file2mnt1, content0))
		test.AssertNoErr(api1.SyncAll())

		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(file2mnt1, &stat))
		size0 := stat.Size

		// Register for all workspace updates so we know when the first
		// update happens after the sync above and therefore when we need to
		// check the newly added file.
		wait := test.workspaceWaitChan(workspaceName)

		// This will create testFile1 which fills the dirty queue with:
		// [testFile1] [dir] [WSR]
		test.AssertNoErr(testutils.PrintToFile(file1mnt1, content1))

		// Now this will modify testFile2, which creates an effective view
		// and results in the following dirty queue:
		// [testFile1] [dir] [WSR] [testFile2]
		test.AssertNoErr(testutils.OverWriteFile(file2mnt1, content1))

		test.AssertNoErr(syscall.Stat(file2mnt1, &stat))
		size1 := stat.Size

		waitForUpdate := func() {
			test.Log("Waiting for update to sync")
			select {
			case <-wait:
				test.Assert(false,
					"Workspace update prior to test ready")
			default:
			}
			<-wait
		}

		verifyFile := func(expectedSize int64, expectedContent string) bool {
			test.AssertNoErr(syscall.Stat(file2mnt0, &stat))
			if stat.Size != expectedSize {
				test.Log("File had unexpected size %d", stat.Size)
				return false
			}

			file, err := os.OpenFile(file2mnt0, os.O_RDONLY, 0777)
			test.AssertNoErr(err)
			defer file.Close()
			test.verifyContentStartsWith(file, expectedContent)
			return true
		}

		// The first update happens when WSR publishes. testFile2 is the only
		// thing left on the dirty queue and the published metadata must show
		// the old size even though the effective view has the new size.
		//
		// The dirty queue is: [testFile2]
		//
		// We haven't accessed this workspace by this instance yet, so we can
		// check immediately.
		waitForUpdate()
		test.Assert(verifyFile(size0, content0),
			"File didn't verify, see above")

		// The second update happens when the WSR publishes again after
		// testFile2 has synced. The effective dirty queue was:
		// [testFile2] [dir] [WSR]
		waitForUpdate()
		close(wait) // Fail on unexpected workspace publishing

		test.WaitFor("Refresh to complete and see second state",
			func() bool { return verifyFile(size1, content1) })
	})
}

func TestPublishedHardlinksToBeConsistent(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		mnt1 := test.qfsInstances[1].config.MountPath + "/"
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + workspaceName
		dir0 := "dir0"
		dir1 := "dir1"
		file1 := "testFile1"
		file2 := "testFile2"
		file3 := "testFile3"
		otherFile := "dir0/otherFile"
		content := "content"

		c := test.TestCtx()
		api1, err := quantumfs.NewApiWithPath(mnt1 + "api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		// Keep a handle to workspace0 to make sure it is refreshed
		wsr0, cleanup := test.GetWorkspaceRoot(workspace0)
		defer cleanup()
		test.Assert(wsr0 != nil, "workspace root does not exist")

		// This marks workspace0 as immutable
		test.markImmutable(c, workspaceName)

		file1fullname1 := fmt.Sprintf("%s/%s/%s", workspace1, dir0, file1)

		file1fullname0 := fmt.Sprintf("%s/%s/%s", workspace0, dir0, file1)
		file2fullname0 := fmt.Sprintf("%s/%s/%s", workspace0, dir0, file2)
		file3fullname0 := fmt.Sprintf("%s/%s/%s", workspace0, dir1, file3)

		test.AssertNoErr(utils.MkdirAll(workspace1+"/"+dir0, 0777))
		test.AssertNoErr(utils.MkdirAll(workspace1+"/"+dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1fullname1, content))
		test.linkFile(workspace1, dir0+"/"+file1, dir0+"/"+file2)
		test.linkFile(workspace1, dir0+"/"+file1, dir1+"/"+file3)
		test.AssertNoErr(api1.SyncWorkspace(workspaceName))
		test.createFile(workspace1, otherFile, 100)
		test.removeFile(workspace1, dir1+"/"+file3)

		<-test.workspaceWaitChan(workspaceName)
		// otherFile must have now appeared as it should be part of the
		// first update
		func() {
			file, err := os.OpenFile(workspace0+"/"+otherFile,
				os.O_RDONLY, 0777)
			defer file.Close()
			test.AssertNoErr(err)
		}()
		// This marks workspace0 mutable again
		test.markMutable(c, workspaceName)
		defer test.remountFilesystem()
		test.AssertNoErr(os.Remove(file3fullname0))
		test.AssertNoErr(os.Remove(file2fullname0))
		file, err := os.OpenFile(file1fullname0, os.O_RDONLY, 0777)
		test.AssertNoErr(err)
		defer file.Close()
		test.verifyContentStartsWith(file, content)
	})
}

func TestFlushAllSorting(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		dirs1 := workspace + "/dir1/dir2/dir3/dir4"
		dirs2 := dirs1 + "/dir5/dir6/dir7/dir8/dir9"
		file1 := dirs2 + "/file1"
		file2 := workspace + "/file2"
		file3 := workspace + "/file3"

		test.AssertNoErr(utils.MkdirAll(dirs2, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, "data"))
		test.AssertNoErr(testutils.PrintToFile(file2, "data"))

		test.SyncAllWorkspaces()

		numberOfPublishes := 0
		publishUpdates := test.workspaceWaitChan(test.RelPath(workspace))
		go func() {
			<-publishUpdates
			numberOfPublishes = 1
			test.Log("Received publish")

			<-publishUpdates
			test.T.Fatalf("Received a second publish update")
		}()

		test.AssertNoErr(syscall.Link(file2, workspace+"/link2"))
		test.AssertNoErr(testutils.PrintToFile(file3, "data"))
		test.AssertNoErr(testutils.PrintToFile(file1, "data2"))

		test.SyncAllWorkspaces()

		test.WaitFor("Workspace to publish", func() bool {
			return numberOfPublishes == 1
		})

		// Allow time for additional publishes to occur and fail in the
		// waiting goroutine.
		time.Sleep(500 * time.Millisecond)
	})
}

func TestRecreateWorkspaceWithContents(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		workspaceName := test.RelPath(workspace)
		fileName1 := workspace + "/file1"
		fileName2 := workspace + "/file2"

		file, err := os.Create(fileName1)
		test.AssertNoErr(err)
		file.Close()

		api := test.getApi()
		test.AssertNoErr(api.DeleteWorkspace(workspaceName))

		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.EnableRootWrite(workspaceName))

		file, err = os.Create(fileName2)
		test.AssertNoErr(err)
		file.Close()

		workspace2 := test.AbsPath(test.branchWorkspace(workspace))
		test.assertNoFile(workspace2 + "/file1")
		test.assertFileExists(workspace2 + "/file2")
	})
}
