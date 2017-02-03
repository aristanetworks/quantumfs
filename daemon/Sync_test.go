// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test Inode syncing to ensure it happens correctly and only happens when it
// supposed to.

import "bufio"
import "bytes"
import "io"
import "io/ioutil"
import "os"
import "strings"
import "strconv"
import "sync/atomic"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

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
		test.startQuantumFs(config)
		workspace := test.newWorkspace()

		// Generate some deterministic, pseudorandom data for a folder
		// structure, treating each number as a command
		data := genData(500)
		totalWritten := 0
		dataWidth := 50
		for i := 0; i < len(data)/dataWidth; i++ {
			fd, err := os.OpenFile(workspace+"/testFile",
				os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			test.assert(err == nil, "Unable to open testFile %s", err)

			toWrite := data[i*dataWidth : (i+1)*dataWidth]
			bufferedWriter := bufio.NewWriter(fd)
			wrote, err := bufferedWriter.Write(toWrite)
			totalWritten += wrote
			test.assert(err == nil, "Unable to write to testFile")

			bufferedWriter.Flush()
			fd.Close()

			test.syncAllWorkspaces()
		}
		test.assert(totalWritten == len(data), "Written mismatch")

		test.api.Close()
		err := test.qfs.server.Unmount()
		test.assert(err == nil, "Failed to unmount during test")

		test.startQuantumFs(config)

		fileRestored, err := ioutil.ReadFile(workspace + "/testFile")
		test.assert(err == nil, "Unable to read file after qfs reload: %s",
			err)
		test.assert(bytes.Equal(data, fileRestored),
			"File contents not completely synced / restored %d %d",
			len(data), len(fileRestored))
	})
}

func TestSyncToDatastore(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()

		// Make an instance of QuantumFs and put things there
		test.startQuantumFs(config)
		workspace := test.newWorkspace()

		// Generate some deterministic, pseudorandom data for a folder
		// structure, treating each number as a command. Ensure genData
		// generates a long enough string of natural numbers to ensure at
		// least a few sync commands occur.
		data := genData(50)
		folderStack := make([]string, 0)
		for i := 0; i < len(data); i++ {
			// and occasionally force a sync
			if data[i] == '0' {
				test.syncAllWorkspaces()
			}

			if data[i] < '3' {
				folderStack = append(folderStack, "/folder")
				folderStr := strings.Join(folderStack, "")
				os.MkdirAll(workspace+folderStr, 0777)
			} else if data[i] < '5' && len(folderStack) > 0 {
				folderStack = folderStack[:len(folderStack)-1]
			} else {
				folderStr := strings.Join(folderStack, "")
				ioutil.WriteFile(workspace+folderStr+"/file"+
					strconv.Itoa(i), []byte(data), os.ModePerm)
			}
		}

		// Sync all of the data we've written
		test.syncAllWorkspaces()

		// Now end quantumfs A, and start B with the same datastore so we
		// can verify that the data was preserved via the datastore
		test.api.Close()
		err := test.qfs.server.Unmount()
		test.assert(err == nil, "Failed to unmount during test")

		test.startQuantumFs(config)

		// Iterate in the exact same way, but this time verifying instead of
		// creating the data
		folderStack = make([]string, 0)
		for i := 0; i < len(data); i++ {
			if data[i] < '3' {
				folderStack = append(folderStack, "/folder")
				folderStr := strings.Join(folderStack, "")

				_, err := os.Stat(workspace + folderStr)
				test.assert(err == nil, "Folder lost (%s): %v",
					workspace+folderStr, err)
			} else if data[i] < '5' && len(folderStack) > 0 {
				folderStack = folderStack[:len(folderStack)-1]
			} else {
				folderStr := strings.Join(folderStack, "")
				fileName := workspace + folderStr + "/file" +
					strconv.Itoa(i)

				fileData, err := ioutil.ReadFile(fileName)

				test.assert(err == nil, "File lost (%s): %s",
					fileName, err)
				test.assert(bytes.Equal(fileData, data),
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

		workspace := test.newWorkspace()

		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/c"

		// Create a directory
		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		file, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating file: %v", err)
		defer file.Close()

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
		test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IRUSR | syscall.S_IWUSR
		expectedPermissions |= syscall.S_IRGRP | syscall.S_IWGRP
		expectedPermissions |= syscall.S_IROTH | syscall.S_IWOTH
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		// Write a small file
		origData := []byte("test text")
		_, err = file.Write(origData)
		test.assert(err == nil, "Error writing to file %v", err)

		// Confirm nothing to this point has synced anything
		setCount := atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount == 0, "Datastore sets occurred! %d", setCount)

		// Confirm the data is correct
		data, err := ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading from file %v", err)
		test.assert(bytes.Equal(data, origData), "Data does not match %v",
			data)
		// ioutil.ReadFile() opens and closes the file. This causes a
		// Flush() which will sync the file. Record the number of datastore
		// writes to compare against later.
		expectedCount := atomic.LoadUint64(&dataStore.setCount)

		// Write a medium file
		err = file.Truncate(20 * 1024 * 1024)
		test.assert(err == nil, "Error extending small file %v", err)

		// Confirm the only sync at this point was the move from small file
		// to medium file.
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount == expectedCount, "Unexpected store writes %d",
			setCount)

		data, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading from file %v", err)
		test.assert(bytes.Equal(data[:len(origData)], origData),
			"Data does not match %v", data)
		expectedCount = atomic.LoadUint64(&dataStore.setCount)

		// Extend the file even further
		origData = []byte("more text")
		_, err = file.WriteAt(origData, 21*1024*1024)
		test.assert(err == nil, "Error writing deep into medium file %v",
			err)
		buf := make([]byte, 100)
		read, err := file.ReadAt(buf, 21*1024*1024)
		test.assert(read == len(origData),
			"Read unexpected number of bytes %d != %d", read,
			len(origData))
		test.assert(err == io.EOF, "Expected end of file! %v", err)
		test.assert(bytes.Equal(buf[:read], origData),
			"Incorrect data read back %v", buf)

		// Now we sync everything and confirm that writes to the datastore
		// happen
		test.syncAllWorkspaces()
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount > expectedCount,
			"Datastore sets didn't happen! %d", setCount)
	})
}
