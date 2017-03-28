// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test various instances of actions which should modify (or not) the ctime and
// mtime.

import "fmt"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/testutils"

func getTimes(path string) (mtime int64, ctime int64) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		msg := fmt.Sprintf("stat call failed: %v", err)
		panic(msg)
	}

	mtime = stat.Mtim.Nano()
	ctime = stat.Ctim.Nano()
	return
}

func getTimeFromFile(file *os.File) (mtime int64, ctime int64) {
	fd := int(file.Fd())
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		msg := fmt.Sprintf("fstat call failed: %v", err)
		panic(msg)
	}
	mtime = stat.Mtim.Nano()
	ctime = stat.Ctim.Nano()
	return
}

func TestTimeChmod(t *testing.T) {
	// Change metadata and confirm mtime isn't changed
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		dirName := workspace + "/dir"
		fileName := workspace + "/file"

		err := os.Mkdir(dirName, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating file: %v", err)
		file.Close()

		// Confirm changing attributes modifies ctime, but not mtime on a
		// directory
		mtimeOrig, ctimeOrig := getTimes(dirName)
		err = os.Chmod(dirName, 0777)
		test.Assert(err == nil, "Error chmod'ing directory: %v", err)
		mtimeNew, ctimeNew := getTimes(dirName)

		test.Assert(mtimeOrig == mtimeNew, "mtime changed for directory")
		test.Assert(ctimeOrig < ctimeNew, "ctime unchanged for directory")

		// Confirm changing attributes modifies ctime, but not mtime on a
		// file
		mtimeOrig, ctimeOrig = getTimes(fileName)
		err = os.Chmod(fileName, 0777)
		test.Assert(err == nil, "Error chmod'ing file: %v", err)
		mtimeNew, ctimeNew = getTimes(fileName)

		test.Assert(mtimeOrig == mtimeNew, "mtime changed for file")
		test.Assert(ctimeOrig < ctimeNew,
			"ctime didn't change for file, %d, %d", ctimeOrig,
			ctimeNew)
	})
}

// Change contents and confirm both ctime and mtime are changed
func TestTimeModification(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		dirName := workspace + "/dir"
		fileName := workspace + "/dir/file"

		err := os.Mkdir(dirName, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)
		mtimeOrig, ctimeOrig := getTimes(dirName)

		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating file: %v", err)
		file.Close()

		mtimeNew, ctimeNew := getTimes(dirName)

		test.Assert(mtimeOrig < mtimeNew, "mtime unchanged for directory")
		test.Assert(ctimeOrig < ctimeNew, "ctime unchanged for directory")

		mtimeOrig, ctimeOrig = getTimes(fileName)
		err = testutils.PrintToFile(fileName, "this is some content")
		test.Assert(err == nil, "Error modifying file: %v", err)
		mtimeNew, ctimeNew = getTimes(fileName)

		test.Assert(mtimeOrig < mtimeNew, "mtime unchanged for file")
		test.Assert(ctimeOrig < ctimeNew, "ctime unchanged for file")
	})
}

// Confirm that if a file inside a directory has it's times change, the directory
// doesn't have it's time change
func TestTimeRecursiveCtime(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		dirName := workspace + "/dir"
		fileName := workspace + "/dir/file"

		err := os.Mkdir(dirName, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating file: %v", err)
		file.Close()

		mtimeOrig, ctimeOrig := getTimes(dirName)

		err = testutils.PrintToFile(fileName, "this is some content")
		test.Assert(err == nil, "Error modifying file: %v", err)

		mtimeNew, ctimeNew := getTimes(dirName)
		test.Assert(mtimeOrig == mtimeNew, "mtime changed for directory")
		test.Assert(ctimeOrig == ctimeNew, "ctime changed for directory")
	})
}

func TestTimeIntraDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/dir"
		testFilename1 := dirName + "/test"
		testFilename2 := dirName + "/test2"

		err := os.Mkdir(dirName, 0777)
		test.Assert(err == nil, "Error creating directory: %v", err)

		fd, err := os.Create(testFilename1)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		mtimeOrig, ctimeOrig := getTimes(dirName)

		err = os.Rename(testFilename1, testFilename2)
		test.Assert(err == nil, "Error renaming file: %v", err)

		mtimeNew, ctimeNew := getTimes(dirName)
		test.Assert(mtimeOrig < mtimeNew, "mtime unchanged for directory")
		test.Assert(ctimeOrig < ctimeNew, "ctime unchanged for directory")
	})
}

func TestTimeInterDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		interDirectoryRename(test)
		workspace := test.newWorkspace()
		testDir1 := workspace + "/dir1"
		testDir2 := workspace + "/dir2"
		testFilename1 := testDir1 + "/test"
		testFilename2 := testDir2 + "/test2"

		err := os.Mkdir(testDir1, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)
		err = os.Mkdir(testDir2, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(testFilename1)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		mtimeOrig1, ctimeOrig1 := getTimes(testDir1)
		mtimeOrig2, ctimeOrig2 := getTimes(testDir2)
		err = os.Rename(testFilename1, testFilename2)
		test.Assert(err == nil, "Error renaming file: %v", err)

		mtimeNew1, ctimeNew1 := getTimes(testDir1)
		mtimeNew2, ctimeNew2 := getTimes(testDir2)

		test.Assert(mtimeOrig1 < mtimeNew1, "mtime unchanged for directory")
		test.Assert(ctimeOrig1 < ctimeNew1, "ctime unchanged for directory")
		test.Assert(mtimeOrig2 < mtimeNew2, "mtime unchanged for directory")
		test.Assert(ctimeOrig2 < ctimeNew2, "ctime unchanged for directory")
	})
}

func TestTimeOrphanedFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)
		defer file.Close()

		data := genData(100 * 1024)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)
		err = os.Remove(testFilename)
		test.Assert(err == nil, "Error unlinking test file: %v", err)

		// Confirm we can still read its times
		mtimeOrig, ctimeOrig := getTimeFromFile(file)
		test.Assert(ctimeOrig != 0, "ctime invalid: %d", ctimeOrig)
		test.Assert(mtimeOrig != 0, "mtime invalid: %d", mtimeOrig)

		// Change the attributes to ensure ctime and not mtime is changed
		err = file.Chmod(0777)
		test.Assert(err == nil, "Error chmod'ing file: %v", err)
		mtime, ctime := getTimeFromFile(file)
		test.Assert(mtimeOrig == mtime, "mtime changed")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")

		// Change the data to ensure both ctime and mtime are changed
		mtimeOrig = mtime
		ctimeOrig = ctime
		data = genData(100 * 1024)
		_, err = file.Seek(100*1024*1024, 0)
		test.Assert(err == nil, "Error rewinding file: %v", err)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		mtime, ctime = getTimeFromFile(file)
		test.Assert(mtimeOrig < mtime, "mtime unchanged")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")
	})
}

func TestTimeHardlinkFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		filename := workspace + "/test"

		// First create a file with some data
		data := genData(2000)
		err := testutils.PrintToFile(filename, string(data))
		test.AssertNoErr(err)

		mtimeOrig, ctimeOrig := getTimes(filename)
		test.Assert(ctimeOrig != 0, "ctime invalid: %d", ctimeOrig)
		test.Assert(mtimeOrig != 0, "mtime invalid: %d", mtimeOrig)

		// Link to ensure ctime and not mtime is changed
		linkname := workspace + "/testlink"
		err = syscall.Link(filename, linkname)
		test.Assert(err == nil, "Error linking file: %v", err)
		mtime, ctime := getTimes(filename)
		test.Assert(mtimeOrig == mtime, "mtime changed")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")

		// Ensure the link shares the same times
		mtimelink, ctimelink := getTimes(linkname)
		test.Assert(mtimelink == mtime, "link mtime changed")
		test.Assert(ctimelink == ctime, "link ctime changed")

		// Change the data to ensure both ctime and mtime are changed
		mtimeOrig = mtime
		ctimeOrig = ctime
		err = testutils.PrintToFile(linkname, string(data))
		test.AssertNoErr(err)

		mtime, ctime = getTimes(filename)
		test.Assert(mtimeOrig < mtime, "mtime unchanged")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")

		// Ensure the link matches
		mtimelink, ctimelink = getTimes(linkname)
		test.Assert(mtimelink == mtime, "link mtime changed")
		test.Assert(ctimelink == ctime, "link ctime changed")
		mtimeOrig = mtime
		ctimeOrig = ctime

		// Change the attributes to ensure ctime and not mtime is changed
		err = os.Chmod(filename, 0777)
		test.AssertNoErr(err)
		mtime, ctime = getTimes(filename)
		test.Assert(mtimeOrig == mtime, "mtime changed")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")
		mtimeOrig = mtime
		ctimeOrig = ctime

		// Remove the link, and ensure that still changes ctime
		err = os.Remove(linkname)
		test.AssertNoErr(err)

		mtime, ctime = getTimes(filename)
		test.Assert(mtimeOrig == mtime, "mtime changed")
		test.Assert(ctimeOrig < ctime, "ctime unchanged")
	})
}
