// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on files such as creation, read and write

import "bytes"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

func TestFileCreation(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)

		err = syscall.Close(fd)
		test.Assert(err == nil, "Error closing fd: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.Assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.Assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestFileWriteBlockSize(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/" + "testwsize"
		file, err := os.Create(testFilename)
		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)
		defer file.Close()

		data := genData(131072)

		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing to new fd: %v", err)
		test.AssertLogContains("operateOnBlocks offset 0 size 131072",
			"Write block size not expected")
	})
}

func TestFileReadWrite(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		//length of test Text should be 37, and not a multiple of readBuf len
		testText := []byte("This is test data 1234567890 !@#^&*()")
		//write the test data in two goes
		textSplit := len(testText) / 2

		workspace := test.newWorkspace()

		testFilename := workspace + "/" + "testrw"
		file, err := os.Create(testFilename)
		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)

		//ensure the Create() handle works
		written := 0
		for written < textSplit {
			var writeIt int
			writeIt, err = file.Write(testText[written:textSplit])
			written += writeIt
			test.Assert(err == nil, "Error writing to new fd: %v", err)
		}

		readLen := 0
		//intentionally make the read buffer small so we do multiple reads
		fullReadBuf := make([]byte, 100)
		readBuf := make([]byte, 4)
		for readLen < written {
			var readIt int
			//note, this also tests read offsets
			readIt, err = file.ReadAt(readBuf, int64(readLen))
			copy(fullReadBuf[readLen:], readBuf[:readIt])
			readLen += readIt
			test.Assert(err == nil || err == io.EOF,
				"Error reading from fd: %v", err)
		}
		test.Assert(bytes.Equal(fullReadBuf[:readLen], testText[:written]),
			"Read and written bytes do not match, %s vs %s",
			fullReadBuf[:readLen], testText[:written])

		err = file.Close()
		test.Assert(err == nil, "Error closing fd: %v", err)

		//now open the file again to trigger Open()
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.Assert(err == nil, "Error opening fd: %v", err)

		//test overwriting past the end of the file with an offset by
		//rewinding back the textSplit
		textSplit -= 2
		written -= 2

		//ensure the Open() handle works
		for written < len(testText) {
			var writeIt int
			//test the offset code path by writing a small bit at a time
			writeTo := len(testText)
			if writeTo > written+4 {
				writeTo = written + 4
			}

			writeIt, err = file.WriteAt(testText[written:writeTo],
				int64(written))
			written += writeIt
			test.Assert(err == nil, "Error writing existing fd: %v", err)
		}

		readLen = 0
		for readLen < written {
			var readIt int
			//note, this also tests read offsets
			readIt, err = file.ReadAt(readBuf, int64(readLen))
			copy(fullReadBuf[readLen:], readBuf[:readIt])
			readLen += readIt
			test.Assert(err == nil || err == io.EOF,
				"Error reading from fd: %v", err)
		}
		test.Assert(bytes.Equal(fullReadBuf[:readLen], testText[:written]),
			"Read and written bytes do not match, %s vs %s",
			fullReadBuf[:readLen], testText[:written])

		err = file.Close()
		test.Assert(err == nil, "Error closing fd: %v", err)

		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.Assert(err == nil, "Error opening fd: %v", err)

		readLen = 0
		for readLen < len(testText) {
			var readIt int
			readIt, err = file.Read(readBuf)
			copy(fullReadBuf[readLen:], readBuf[:readIt])
			readLen += readIt
			test.Assert(err != io.EOF || err == nil,
				"Error reading from fd: %v", err)
		}
		test.Assert(bytes.Equal(fullReadBuf[:readLen], testText[:written]),
			"Read and written bytes do not match, %s vs %s",
			fullReadBuf[:readLen], testText[:written])

		err = file.Close()
		test.Assert(err == nil, "Error closing fd: %v", err)
	})
}

func TestFileDescriptorPermissions(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testDir := workspace + "/testDir"
		testFilename := testDir + "/test"

		err := syscall.Mkdir(testDir, 0777)
		test.Assert(err == nil, "Error creating directories: %v", err)

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

		// Now create the test file
		fd, err := syscall.Creat(testFilename, 0000)
		test.Assert(err == nil, "Error creating file: %s %v", testFilename,
			err)
		syscall.Close(fd)
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions := modeToPermissions(stat.Mode, 0x777)
		test.Assert(permissions == 0x0,
			"Creating with mode not preserved, %d vs 0000", permissions)

		//test write only
		err = syscall.Chmod(testFilename, 0222)
		test.Assert(err == nil, "Error chmod-ing test file: %v", err)
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions = modeToPermissions(stat.Mode, 0)
		test.Assert(permissions == 0222,
			"Chmodding not working, %d vs 0222", permissions)

		var file *os.File
		//ensure we can't read the file, only write
		file, err = os.Open(testFilename)
		test.Assert(file == nil && err != nil,
			"Able to open write-only file for read")
		test.Assert(os.IsPermission(err),
			"Expected permission error not returned: %v", err)
		file.Close()

		file, err = os.OpenFile(testFilename, os.O_WRONLY, 0x2)
		test.Assert(file != nil && err == nil,
			"Unable to open file only for writing with permissions")
		file.Close()

		//test read only
		err = syscall.Chmod(testFilename, 0444)
		test.Assert(err == nil, "Error chmod-ing test file: %v", err)
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions = modeToPermissions(stat.Mode, 0)
		test.Assert(permissions == 0444,
			"Chmodding not working, %d vs 0444", permissions)

		file, err = os.OpenFile(testFilename, os.O_WRONLY, 0x2)
		test.Assert(file == nil && err != nil,
			"Able to open read-only file for write")
		test.Assert(os.IsPermission(err),
			"Expected permission error not returned: %v", err)
		file.Close()

		file, err = os.Open(testFilename)
		test.Assert(file != nil && err == nil,
			"Unable to open file only for reading with permissions")
		file.Close()
	})
}

func TestRootFileDescriptorPermissions(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		fd, err := syscall.Creat(testFilename, 0000)
		test.Assert(err == nil, "Error creating file: %s %v", testFilename,
			err)
		syscall.Close(fd)
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions := modeToPermissions(stat.Mode, 0x777)
		test.Assert(permissions == 0x0,
			"Creating with mode not preserved, %d vs 0000", permissions)

		//test write only
		err = syscall.Chmod(testFilename, 0222)
		test.Assert(err == nil, "Error chmod-ing test file: %v", err)
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions = modeToPermissions(stat.Mode, 0)
		test.Assert(permissions == 0222,
			"Chmodding not working, %d vs 0222", permissions)

		var file *os.File
		//ensure we can't read the file, only write
		file, err = os.Open(testFilename)
		test.Assert(file != nil && err == nil,
			"root unable to open write-only file for read")
		file.Close()

		file, err = os.OpenFile(testFilename, os.O_WRONLY, 0x2)
		test.Assert(file != nil && err == nil,
			"Unable to open file only for writing with permissions")
		file.Close()

		//test read only
		err = syscall.Chmod(testFilename, 0444)
		test.Assert(err == nil, "Error chmod-ing test file: %v", err)
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		permissions = modeToPermissions(stat.Mode, 0)
		test.Assert(permissions == 0444,
			"Chmodding not working, %d vs 0444", permissions)

		file, err = os.OpenFile(testFilename, os.O_WRONLY, 0x2)
		test.Assert(file != nil && err == nil,
			"root unable to open read-only file for write")
		file.Close()

		file, err = os.Open(testFilename)
		test.Assert(file != nil && err == nil,
			"Unable to open file only for reading with permissions")
		file.Close()
	})
}

func TestFileSizeChanges(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/" + "test"

		testText := "TestString"
		err := testutils.PrintToFile(testFilename, testText)
		test.Assert(err == nil, "Error writing to new fd: %v", err)

		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Failed reading from file: %v", err)
		test.Assert(string(output) == testText,
			"File contents incorrect: '%s'", string(output))

		err = os.Truncate(testFilename, 4)
		test.Assert(err == nil, "Problem truncating file")

		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil && string(output) == testText[:4],
			"Truncated file contents not what's expected")

		err = os.Truncate(testFilename, 8)
		test.Assert(err == nil, "Unable to extend file size with SetAttr")

		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil &&
			string(output) == testText[:4]+"\x00\x00\x00\x00",
			"Extended file isn't filled with a hole: '%s'",
			string(output))

		// Shrink it again to ensure double truncates work
		err = os.Truncate(testFilename, 6)
		test.Assert(err == nil, "Problem truncating file")

		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil &&
			string(output) == testText[:4]+"\x00\x00",
			"Extended file isn't filled with a hole: '%s'",
			string(output))

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == 6,
			"File size didn't match expected: %d", stat.Size)

		err = testutils.PrintToFile(testFilename, testText)
		test.Assert(err == nil, "Error writing to new fd: %v", err)

		output, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil &&
			string(output) == testText[:4]+"\x00\x00"+testText,
			"Append to file with a hole is incorrect: '%s'",
			string(output))

		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == int64(6+len(testText)),
			"File size change not preserve with file append: %d",
			stat.Size)
	})
}

func TestFileDescriptorDirtying(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		// Create a file and determine its inode numbers
		workspace := test.newWorkspace()
		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Ino >= quantumfs.InodeIdReservedEnd,
			"File had reserved inode number %d", stat.Ino)

		// Find the matching FileHandle
		descriptors := test.fileDescriptorFromInodeNum(stat.Ino)
		test.Assert(len(descriptors) == 1,
			"Incorrect number of fds found 1 != %d", len(descriptors))
		fileDescriptor := descriptors[0]
		file := fileDescriptor.file

		// Save the workspace rootId, change the File key, simulating
		// changing the data, then mark the matching FileDescriptor dirty.
		// This should trigger a refresh up the hierarchy and, because we
		// currently do not support delayed syncing, change the workspace
		// rootId and mark the fileDescriptor clean.
		oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		c := test.newCtx()
		_, err = file.accessor.writeBlock(c, 0, 0, []byte("update"))
		test.Assert(err == nil, "Failure modifying small file")
		fileDescriptor.dirty(c)

		test.syncAllWorkspaces()
		newRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		test.Assert(oldRootId != newRootId, "Workspace rootId didn't change")

		syscall.Close(fd)
	})
}

// Test file metadata updates
func TestFileAttrUpdate(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		api := test.getApi()

		src := test.newWorkspace()
		src = test.relPath(src)

		dst := "dst/attrupdate/test"

		// First create a file
		testFile := test.absPath(src + "/" + "test")
		fd, err := os.Create(testFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		// Then apply a SetAttr only change
		os.Truncate(testFile, 5)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		testFile = test.absPath(dst + "/" + "test")
		// Ensure the new workspace has the correct file attributes
		var stat syscall.Stat_t
		err = syscall.Stat(testFile, &stat)
		test.Assert(err == nil, "Workspace copy doesn't have file")
		test.Assert(stat.Size == 5, "Workspace copy attr Size not updated")

		// Read the data and ensure it's what we expected
		var output []byte
		output, err = ioutil.ReadFile(testFile)
		test.Assert(string(output) == "\x00\x00\x00\x00\x00",
			"Workspace doesn't fully reflect attr Size change %v",
			output)
	})
}

func TestFileAttrWriteUpdate(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		api := test.getApi()

		src := test.newWorkspace()
		src = test.relPath(src)

		dst := "dst/attrwriteupdate/test"

		// First create a file
		testFile := test.absPath(src + "/" + "test")
		fd, err := os.Create(testFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		// Then apply a SetAttr only change
		os.Truncate(testFile, 5)

		// Add some extra data to it
		testText := "ExtraData"
		err = testutils.PrintToFile(testFile, testText)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		testFile = test.absPath(dst + "/" + "test")
		// Ensure the new workspace has the correct file attributes
		var stat syscall.Stat_t
		err = syscall.Stat(testFile, &stat)
		test.Assert(err == nil, "Workspace copy doesn't have file")
		test.Assert(stat.Size == int64(5+len(testText)),
			"Workspace copy attr Size not updated")

		// Read the data and ensure it's what we expected
		var output []byte
		output, err = ioutil.ReadFile(testFile)
		test.Assert(string(output) == "\x00\x00\x00\x00\x00"+testText,
			"Workspace doesn't fully reflect file contents")
	})
}

func TestSmallFileZero(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"

		data := genData(10 * 1024)
		err := testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Error writing tiny data to new fd")

		os.Truncate(testFilename, 0)
		test.Assert(test.FileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.Assert(len(output) == 0, "Empty file not really empty")
		test.Assert(err == nil, "Unable to read from empty file")
	})
}

func TestFileAccessAfterUnlink(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)

		data := genData(100 * 1024)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		// Then confirm we can read back from it fine while it is still
		// linked
		input := make([]byte, 100*1024)
		_, err = file.Seek(0, 0)
		test.Assert(err == nil, "Error rewinding file: %v", err)
		_, err = file.Read(input)
		test.Assert(err == nil, "Error reading from file: %v", err)
		test.Assert(bytes.Equal(data, input), "Didn't read same bytes back!")

		// Then Unlink the file and confirm we can still read and write to it
		err = os.Remove(testFilename)
		test.Assert(err == nil, "Error unlinking test file: %v", err)

		_, err = file.Seek(0, 0)
		test.Assert(err == nil, "Error rewinding file: %v", err)
		_, err = file.Read(input)
		test.Assert(err == nil, "Error reading from file: %v", err)
		test.Assert(bytes.Equal(data, input), "Didn't read same bytes back!")

		// Extend the file and read again
		data = genData(100 * 1024)
		_, err = file.Seek(100*1024*1024, 0)
		test.Assert(err == nil, "Error rewinding file: %v", err)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		_, err = file.Seek(100*1024*1024, 0)
		test.Assert(err == nil, "Error rewinding file: %v", err)
		_, err = file.Read(input)
		test.Assert(err == nil, "Error reading from file: %v", err)
		test.Assert(bytes.Equal(data, input), "Didn't read same bytes back!")

		file.Close()
	})
}

func TestSmallFileReadPastEnd(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)

		data := genData(100 * 1024)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)

		// Then confirm we can read back past the data and get the correct
		// EOF return value.
		input := make([]byte, 100*1024)
		_, err = file.ReadAt(input, 100*1024)
		test.Assert(err == io.EOF, "Expected EOF got: %v", err)

		file.Close()
	})
}

func TestFileStatBlockCount(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		// First create a file with some data
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)

		data := genData(1024)
		_, err = file.Write(data)
		test.Assert(err == nil, "Error writing data to file: %v", err)
		defer file.Close()

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		// stat.Blocks must always be in terms of 512B blocks
		test.Assert(uint64(stat.Blocks) == utils.BlocksRoundUp(uint64(stat.Size),
			uint64(512)),
			"Blocks is not in terms of 512B blocks. Blocks %v Size %v",
			stat.Blocks, stat.Size)
	})
}

func TestFileReparentRace(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		workspace := test.newWorkspace()

		var stat syscall.Stat_t
		iterations := 100
		for i := 0; i < iterations; i++ {
			filename := fmt.Sprintf(workspace+"/file%d", i)
			file, err := os.Create(filename)
			test.AssertNoErr(err)

			file.WriteString("this is file data")

			// Leave the file handle open so it gets orphaned. We now
			// want to race the parent change with getting the parent
			go syscall.Stat(filename, &stat)
			go os.Remove(filename)

			file.Close()
		}
	})
}
