// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test Inode syncing to ensure it happens correctly and only happens when it
// supposed to.

import "bytes"
import "io"
import "io/ioutil"
import "os"
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

// Put in place a proxy dataStore which counts the stores we make, then create a few
// directories and file and put some data into those files. During all this we do not
// expect any writes to be made into the datastore. Finally sync all the workspaces
// and confirm some writes happened.
func TestNoImplicitSync(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
		dataStore := setCountingDataStore{
			DataStore: test.qfs.c.dataStore.durableStore,
		}
		test.qfs.c.dataStore.durableStore = &dataStore

		workspace := test.nullWorkspace()
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
		expectedPermissions |= syscall.S_IRUSR | syscall.S_IRGRP | syscall.S_IROTH
		expectedPermissions |= syscall.S_IWUSR | syscall.S_IWGRP | syscall.S_IWOTH
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		// Write a small file
		origData := []byte("test text")
		_, err = file.Write(origData)
		test.assert(err == nil, "Error writing to file %v", err)
		data, err := ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading from file %v", err)
		test.assert(bytes.Equal(data, origData), "Data does not match %v",
			data)

		// Confirm nothing to this point has synced anything
		setCount := atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount == 0, "Datastore sets occurred! %d", setCount)

		// Write a medium file
		err = file.Truncate(20 * 1024 * 1024)
		test.assert(err == nil, "Error extending small file %v", err)
		data, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading from file %v", err)
		test.assert(bytes.Equal(data[:len(origData)], origData),
			"Data does not match %v", data)

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

		// Confirm the only sync at this point was the move from small file
		// to medium file.
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount == 1, "Expected one store write, got %d",
			setCount)

		// Now we sync everything and confirm that writes to the datastore
		// happen
		test.syncAllWorkspaces()
		setCount = atomic.LoadUint64(&dataStore.setCount)
		test.assert(setCount != 0, "Datastore sets didn't happen! %d", setCount)
	})
}
