// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that inodes can be Forgotten and re-accessed

import "bytes"
import "io/ioutil"
import "os"
import "strconv"
import "syscall"
import "testing"
import "time"

func remountFilesystem(test *testHelper) {
	test.log("Remounting filesystem")
	err := syscall.Mount("", test.tempDir+"/mnt", "", syscall.MS_REMOUNT, "")
	test.assert(err == nil, "Unable to force vfs to drop dentry cache: %v", err)
}

func TestForgetOnDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/dir", 0777)

		numFiles := 10
		data := genData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := printToFile(workspace+"/dir/file"+strconv.Itoa(i),
				string(data))
			test.assert(err == nil, "Error creating small file")
		}

		// Now force the kernel to drop all cached inodes
		remountFilesystem(test)

		test.assertLogContains("Forgetting",
			"No inode forget triggered during dentry drop.")

		// Now read all the files back to make sure we still can
		for i := 0; i < numFiles; i++ {
			var readBack []byte
			readBack, err := ioutil.ReadFile(workspace + "/dir/file" +
				strconv.Itoa(i))
			test.assert(bytes.Equal(readBack, data),
				"File contents not preserved after Forget")
			test.assert(err == nil, "Unable to read file after Forget")
		}
	})
}

func TestForgetOnWorkspaceRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		numFiles := 10
		data := genData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := printToFile(workspace+"/file"+strconv.Itoa(i),
				string(data))
			test.assert(err == nil, "Error creating small file")
		}

		// Now force the kernel to drop all cached inodes
		remountFilesystem(test)

		test.assertLogContains("Forgetting",
			"No inode forget triggered during dentry drop.")

		// Now read all the files back to make sure we still can
		for i := 0; i < numFiles; i++ {
			var readBack []byte
			readBack, err := ioutil.ReadFile(workspace + "/file" +
				strconv.Itoa(i))
			test.assert(bytes.Equal(readBack, data),
				"File contents not preserved after Forget")
			test.assert(err == nil, "Unable to read file after Forget")
		}
	})
}

func TestMultipleLookupCount(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()
		config.CacheTimeSeconds = 0
		config.CacheTimeNsecs = 100000
		test.startQuantumFs(config)

		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		file, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating file: %v", err)

		time.Sleep(300 * time.Millisecond)

		file2, err := os.Open(testFilename)
		test.assert(err == nil, "Error opening file readonly")

		file.Close()
		file2.Close()
		// Wait for the closes to bubble up to QuantumFS
		time.Sleep(10 * time.Millisecond)

		// Forget Inodes
		remountFilesystem(test)

		test.assertLogContains("Looked up 2 Times",
			"Failed to cause a second lookup")
		test.assertLogContains("Forgetting inode with lookupCount of 2",
			"Inode with second lookup not forgotten")
	})
}

// QuantumFS doesn't currently implement hardlinks correctly. Instead of returning
// the original inode in the Link() call, it returns a new inode. This doesn't seem
// to bother the kernel and it distributes the lookup counts between those two inodes
// as one would expect. However, this may change and this test should start failing
// if that happens.
func TestLookupCountHardlinks(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()
		config.CacheTimeSeconds = 0
		config.CacheTimeNsecs = 100000
		test.startQuantumFs(config)

		workspace := test.newWorkspace()
		testFilename := workspace + "/test"
		linkFilename := workspace + "/link"

		file, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating file: %v", err)

		err = os.Link(testFilename, linkFilename)
		test.assert(err == nil, "Error creating hardlink")

		file.Close()

		// Forget Inodes
		remountFilesystem(test)

		test.assertLogDoesNotContain("Looked up 2 Times",
			"Failed to cause a second lookup")
	})
}
