// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on extended attributes

import "bytes"
import "fmt"
import "os"
import "strings"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

func TestExtendedAttrReadWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		attrNoData := "user.nodata"
		attrData := "user.data"
		attrDataData := []byte("extendedattributedata")

		err = syscall.Setxattr(testFilename, attrNoData, []byte{}, 0)
		test.Assert(err == nil, "Error setting nodata XAttr: %v", err)

		err = syscall.Setxattr(testFilename, attrData, attrDataData, 0)
		test.Assert(err == nil, "Error setting data XAttr: %v", err)

		// Now confirm we can read them out again
		data := make([]byte, 100)

		size, err := syscall.Getxattr(testFilename, attrNoData, data)
		test.Assert(err == nil, "Error reading nodata XAttr: %v", err)
		test.Assert(size == 0, "nodata XAttr size not zero: %d", size)

		size, err = syscall.Getxattr(testFilename, attrData, data)
		test.Assert(err == nil, "Error reading data XAttr: %v", err)
		test.Assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.Assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data,
			attrDataData)

		// Finally confirm we can overwrite an attribute and read it back
		data = make([]byte, 100)
		attrDataData = []byte("extendedattributedata2")
		err = syscall.Setxattr(testFilename, attrData, attrDataData, 0)
		test.Assert(err == nil, "Error setting data XAttr: %v", err)

		size, err = syscall.Getxattr(testFilename, attrData, data)
		test.Assert(err == nil, "Error reading data XAttr: %v", err)
		test.Assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.Assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data,
			attrDataData)
	})
}

func TestExtendedAttrList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		// Confirm no attributes
		data := make([]byte, 64000)
		size, err := syscall.Listxattr(testFilename, data)
		test.Assert(err == nil, "Error listing XAttr: %v", err)

		// Add a bunch of attributes
		const N = 100
		for i := 0; i < N; i++ {
			attrName := fmt.Sprintf("user.attr%d", i)
			err = syscall.Setxattr(testFilename, attrName, []byte{}, 0)
			test.Assert(err == nil, "Error setting XAttr %s: %v",
				attrName, err)
		}

		// Confirm they are all there
		data = make([]byte, 64000)
		size, err = syscall.Listxattr(testFilename, data)
		test.Assert(err == nil, "Error listing XAttr: %v", err)
		test.Assert(size > 0, "Expected XAttr, but didn't find any")
		test.Assert(size <= len(data), "XAttr names overflowed buffer")

		// Remove the case of the virtual extended attribute: key
		data = data[:size]
		names := bytes.Split(data, []byte("\x00"))
		names = names[:len(names)-1] // Remove empty last element
		test.Assert(len(names) == N, "Fewer XAttr than expected: %d != %d",
			len(names), N)

		for _, nameBytes := range names {
			name := string(nameBytes)
			test.Assert(strings.HasPrefix(name, "user.attr"),
				"Incorrect XAttr name %s", name)
		}
	})
}

func TestExtendedAttrReadNonExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		data := make([]byte, 100)

		size, err := syscall.Getxattr(testFilename, "user.does_not_exist",
			data)
		test.Assert(err != nil, "Expected error reading nodata XAttr")
		test.Assert(strings.Contains(err.Error(), "no data available"),
			"Expected no data available: %v", err)
		test.Assert(size <= 0, "Non-zero size with non-existant XAttr: %d",
			size)
	})
}

func TestExtendedAttrRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		verify := func(N int) {
			// Confirm they are all there
			data := make([]byte, 64000)
			size, err := syscall.Listxattr(testFilename, data)
			test.Assert(err == nil, "Error listing XAttr: %v", err)
			test.Assert(size > 0, "Expected XAttr, but didn't find any")
			test.Assert(size <= len(data),
				"XAttr names overflowed buffer")

			// Remove all the cases of the virtual extended attribute:
			// key
			data = data[:size]
			names := bytes.Split(data, []byte("\x00"))
			names = names[:len(names)-1] // Remove empty last element
			test.Assert(len(names) == N,
				"Fewer XAttr than expected: %d != %d",
				len(names), N)

			nameSet := make(map[string]bool)
			for _, nameBytes := range names {
				name := string(nameBytes)
				test.Assert(strings.HasPrefix(name, "user.attr"),
					"Incorrect XAttr name %s", name)
				nameSet[name] = true
			}
			// Ensure we have no duplicates
			test.Assert(len(nameSet) == N, "Duplicated names retrieved")
		}

		// Add a bunch of attributes
		N := 100
		for i := 0; i < N; i++ {
			attrName := fmt.Sprintf("user.attr%d", i)
			err = syscall.Setxattr(testFilename, attrName, []byte{}, 0)
			test.Assert(err == nil, "Error setting XAttr %s: %v",
				attrName, err)
		}

		// Delete a non-existent attribute
		err = syscall.Removexattr(testFilename, "user.does_not_exist")
		test.Assert(err != nil,
			"Expected error when deleting non-existent XAttr")
		test.Assert(strings.Contains(err.Error(), "no data available"),
			"Expected error 'no data available': %s", err.Error())

		// Delete an attribute from the middle
		err = syscall.Removexattr(testFilename, "user.attr50")
		test.Assert(err == nil, "Error when removing XAttr from middle: %v",
			err)
		N--
		verify(N)

		// Delete an attribute from the end
		err = syscall.Removexattr(testFilename, "user.attr99")
		test.Assert(err == nil, "Error when removing XAttr from end: %v",
			err)
		N--
		verify(N)
	})
}

func matchXAttrExtendedKey(path string, extendedKey []byte,
	test *testHelper, Type quantumfs.ObjectType) {

	key, type_, size, err := quantumfs.DecodeExtendedKey(string(extendedKey))
	test.Assert(err == nil, "Error decompressing the packet")

	// Extract the internal ObjectKey from QuantumFS
	var stat syscall.Stat_t
	err = syscall.Stat(path, &stat)
	test.Assert(err == nil, "Error stat'ing test type %d: %v", Type, err)
	var id InodeId
	id = InodeId(stat.Ino)
	inode := test.qfs.inodes[id]
	record, err := inode.parentGetChildRecordCopy(&test.qfs.c, id)

	// Verify the type and key matching
	test.Assert(type_ == Type && size == record.Size() &&
		bytes.Equal(key.Value(), record.ID().Value()),
		"Error getting the key: %v with size of %d-%d, keys of %v-%v",
		err, Type, type_, key.Value(), record.ID().Value())
}

// Verify the get XAttr function for the self-defined extended key
func TestXAttrExtendedKeyGet(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.newWorkspace()
		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		syscall.Close(fd)

		dirName := workspace + "/dir"
		err = syscall.Mkdir(dirName, 0124)
		test.Assert(err == nil, "Error creating a directory: %v", err)

		linkName := workspace + "/link"
		err = syscall.Symlink(testFilename, linkName)
		test.Assert(err == nil, "Error creating a symlink: %v", err)

		spName := workspace + "/special"
		err = syscall.Mknod(spName, syscall.S_IFIFO|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Error creating a special file: %v", err)

		dst := make([]byte, quantumfs.ExtendedKeyLength)

		// Check the non-existing file
		nonExist := workspace + "/noExist"
		sz, err := syscall.Getxattr(nonExist, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == syscall.ENOENT,
			"Incorrect error getting XAttr from a non-existing file %v",
			err)

		// Try to get extended key for work space root and expected to get
		// ENOATTR (alias of ENODATA), which verifies GetXAttrSize return an
		// appropriate status
		sz, err = syscall.Getxattr(workspace, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == syscall.ENODATA,
			"Incorrect error getting error message: %v, %d", err, sz)

		// Check the file
		sz, err = syscall.Getxattr(testFilename, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the file key: %v with a size of %d",
			err, sz)

		matchXAttrExtendedKey(testFilename, dst, test,
			quantumfs.ObjectTypeSmallFile)

		// check the directory
		sz, err = syscall.Getxattr(dirName, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the directory key: %v with a size of %d",
			err, sz)

		matchXAttrExtendedKey(dirName, dst, test,
			quantumfs.ObjectTypeDirectoryEntry)

		// check the symlink
		sz, err, dst = utils.LGetXattr(linkName, quantumfs.XAttrTypeKey,
			quantumfs.ExtendedKeyLength)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the symlink key: %v with a size of %d",
			err, sz)

		key, type_, size, err := quantumfs.DecodeExtendedKey(string(dst))
		test.Assert(err == nil, "Error decompressing the packet")

		// Extract the internal ObjectKey from QuantumFS
		var stat syscall.Stat_t
		err = syscall.Lstat(linkName, &stat)
		test.Assert(err == nil, "Error stat'ing symlink: %v", err)
		id := InodeId(stat.Ino)
		inode := test.qfs.inode(&test.qfs.c, id)
		record, err := inode.parentGetChildRecordCopy(&test.qfs.c, id)

		// Verify the type and key matching
		test.Assert(type_ == quantumfs.ObjectTypeSymlink &&
			size == record.Size() &&
			bytes.Equal(key.Value(), record.ID().Value()),
			"Error getting the link key: %v with %d, keys of %v-%v",
			err, type_, key.Value(), record.ID().Value())

		// check the special
		sz, err = syscall.Getxattr(spName, quantumfs.XAttrTypeKey, dst)
		test.Assert(err == nil && sz == quantumfs.ExtendedKeyLength,
			"Error getting the special key: %v with a size of %d",
			err, sz)
		matchXAttrExtendedKey(spName, dst, test, quantumfs.ObjectTypeSpecial)
	})
}

// Verify the set/remove XAttr function for extended key is illegal
func TestXAttrTypeKeySetRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.newWorkspace()
		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		syscall.Close(fd)

		// Reset the key
		data := []byte("1234567890abcdefghijk700000008")
		err = syscall.Setxattr(testFilename, quantumfs.XAttrTypeKey, data, 0)
		test.Assert(err == syscall.EPERM,
			"Incorrect error finishing illegal SetXAttr: %v", err)

		// Remove the key
		err = syscall.Removexattr(testFilename, quantumfs.XAttrTypeKey)
		test.Assert(err == syscall.EPERM,
			"Incorrect error finishing illegal RemoveXAttr: %v", err)
	})
}

// Verify list XAttr function will attach extended key behind the real extended
// attributes
func TestXAttrTypeKeyList(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.newWorkspace()
		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.Assert(err == nil, "Error creating a small file: %v", err)

		data := []byte("TestOne")
		data2 := []byte("TestTwo")

		err = syscall.Setxattr(testFilename, "security.one", data, 0)
		test.Assert(err == nil, "Error setting XAttr: %v", err)

		err = syscall.Setxattr(testFilename, "security.two", data2, 0)
		test.Assert(err == nil, "Error setting XAttr: %v", err)

		dst := make([]byte, 64)
		_, err = syscall.Listxattr(testFilename, dst)
		test.Assert(err == nil &&
			bytes.Contains(dst, []byte("security.one")) &&
			bytes.Contains(dst, []byte("security.two")),
			"Error listing XAttr: %v with content of %s",
			err, dst)
	})
}

func TestExtendedKeyDirtyChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/dir"
		fileName := dirName + "/file"

		data := genData(500)

		err := syscall.Mkdir(dirName, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		file, err := os.Create(fileName)
		test.Assert(err == nil, "Failed to create file: %v", err)

		test.syncAllWorkspaces()

		buf := make([]byte, quantumfs.ExtendedKeyLength)
		_, err = syscall.Getxattr(dirName, quantumfs.XAttrTypeKey, buf)
		test.Assert(err == nil, "Failed fetching extended key: %v", err)
		dirKey1 := string(buf)

		written, err := file.Write(data)
		test.Assert(err == nil, "Failed writing data: %v", err)
		test.Assert(written == len(data), "Didn't write enough %d", written)
		file.Close()

		_, err = syscall.Getxattr(dirName, quantumfs.XAttrTypeKey, buf)
		test.Assert(err == nil, "Failed fetching extended key: %v", err)
		dirKey2 := string(buf)

		test.Assert(dirKey1 != dirKey2, "Dirty child not synced!")
	})
}

var initialXAttrNames []string
var initialXAttrValues []string

func init() {
	for _, name := range [...]string{"user.a1", "user.a2", "user.a3"} {
		initialXAttrNames = append(initialXAttrNames, name)
	}

	for _, value := range [...]string{"one", "two", "three"} {
		initialXAttrValues = append(initialXAttrValues, value)
	}
}

func initOrphanedFileExtendedAttributes(test *testHelper) (fd int) {
	// Create the file
	workspace := test.newWorkspace()
	filename := workspace + "/file"
	fd, err := syscall.Creat(filename, 0777)
	test.Assert(err == nil, "Error creating file: %v", err)

	// Add some initial attributes
	for i, name := range initialXAttrNames {
		value := initialXAttrValues[i]

		err := syscall.Setxattr(filename, name, []byte(value), 0)
		test.Assert(err == nil, "Error setting initial attributes: %v", err)
	}

	err = syscall.Unlink(filename)
	test.Assert(err == nil, "Error unlinking file: %v", err)

	return fd
}

func TestOrphanedFileXAttrList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		fd := initOrphanedFileExtendedAttributes(test)
		defer syscall.Close(fd)
		_, err, list := utils.FListXattr(fd, 1024)

		test.Assert(err == nil, "Error listing XAttrs: %v", err)

		for _, name := range initialXAttrNames {
			test.Assert(bytes.Contains(list, []byte(name)),
				"List doesn't contain attribute: %s", name)
		}
	})
}

func TestOrphanedFileXAttrGet(t *testing.T) {
	runTest(t, func(test *testHelper) {
		fd := initOrphanedFileExtendedAttributes(test)
		defer syscall.Close(fd)

		for i, name := range initialXAttrNames {
			value := initialXAttrValues[i]

			_, err, data := utils.FGetXattr(fd, name, 1024)
			test.Assert(err == nil, "Error retrieving attr: %v", err)

			str := string(data)
			test.Assert(str == value, "Invalid value %s (%s) for %s",
				str, value, name)
		}

		_, err, _ := utils.FGetXattr(fd, "user.doesnotexist", 1024)
		test.Assert(err != nil && err == syscall.ENODATA,
			"Succeeded retrieving non-existant XAttr")
	})
}

func TestOrphanFileXAttrSet(t *testing.T) {
	runTest(t, func(test *testHelper) {
		fd := initOrphanedFileExtendedAttributes(test)
		defer syscall.Close(fd)

		err := utils.FSetXattr(fd, "user.new", []byte("new"), 0)
		test.Assert(err == nil, "Error creating XAttr: %v", err)

		err = utils.FSetXattr(fd, "user.a1", []byte("overwritten"), 0)
		test.Assert(err == nil, "Error overwriting XAttr: %v", err)

		_, err, list := utils.FListXattr(fd, 1024)
		test.Assert(err == nil, "Error listing XAttrs: %v", err)

		for i, name := range initialXAttrNames {
			value := initialXAttrValues[i]

			if name == initialXAttrNames[0] {
				value = "overwritten"
			}

			test.Assert(bytes.Contains(list, []byte(name)),
				"XAttr list did not contain name %s", name)

			_, err, data := utils.FGetXattr(fd, name, 1024)
			test.Assert(err == nil, "Error fetching XAttr: %v", err)
			test.Assert(string(data) == value, "Value mismatch %s - %s",
				string(data), value)
		}

		test.Assert(bytes.Contains(list, []byte("user.new")),
			"New XAttr not present")
		_, err, data := utils.FGetXattr(fd, "user.new", 1024)
		test.Assert(err == nil, "Error fetching new XAttr: %v", err)
		test.Assert(string(data) == "new", "New attribute value wrong %s",
			string(data))
	})
}

func TestOrphanFileXAttrRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		fd := initOrphanedFileExtendedAttributes(test)
		defer syscall.Close(fd)

		err := utils.FSetXattr(fd, "user.new", []byte("new"), 0)
		test.Assert(err == nil, "Error creating XAttr: %v", err)

		err = utils.FRemoveXattr(fd, "user.new")
		test.Assert(err == nil, "Error removing new XAttr: %v", err)

		err = utils.FRemoveXattr(fd, initialXAttrNames[0])
		test.Assert(err == nil, "Error removing existing XAttr: %v", err)

		_, err, list := utils.FListXattr(fd, 1024)
		test.Assert(err == nil, "Error listing XAttrs: %v", err)

		for _, name := range initialXAttrNames {

			if name == initialXAttrNames[0] {
				test.Assert(!bytes.Contains(list, []byte(name)),
					"XAttr list contain %s", name)
			} else {
				test.Assert(bytes.Contains(list, []byte(name)),
					"XAttr list did not contain name %s", name)
			}
		}

		test.Assert(!bytes.Contains(list, []byte("user.new")),
			"XAttr list contains user.new")

		err = utils.FRemoveXattr(fd, "user.doesnotexist")
		test.Assert(err != nil && err == syscall.ENODATA,
			"Successed removing non-existant XAttr")
	})
}

func TestHardlinkXAttr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		attrNoData := "user.nodata"
		attrPrevData := "user.prevdata"
		attrData := "user.data"
		attrDataData := []byte("extendedattributedata")

		err := utils.MkdirAll(workspace+"/subdir", 0777)
		test.AssertNoErr(err)

		filename := workspace + "/subdir/file"
		err = testutils.PrintToFile(filename, string(genData(2000)))
		test.AssertNoErr(err)

		err = syscall.Setxattr(filename, attrPrevData, attrDataData, 0)
		test.AssertNoErr(err)

		linkFile := workspace + "/subdir/link"
		err = syscall.Link(workspace+"/subdir/file", linkFile)
		test.AssertNoErr(err)

		// Write
		err = syscall.Setxattr(linkFile, attrNoData, []byte{}, 0)
		test.AssertNoErr(err)

		err = syscall.Setxattr(linkFile, attrData, attrDataData, 0)
		test.AssertNoErr(err)

		// Read
		data := make([]byte, 100)
		size, err := syscall.Getxattr(linkFile, attrNoData, data)
		test.AssertNoErr(err)
		test.Assert(size == 0, "nodata XAttr size not zero: %d", size)

		size, err = syscall.Getxattr(linkFile, attrData, data)
		test.AssertNoErr(err)
		test.Assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.Assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data,
			attrDataData)

		// List
		size, err = syscall.Listxattr(linkFile, data)
		test.AssertNoErr(err)
		test.Assert(bytes.Contains(data, []byte(attrNoData)),
			"Empty xattr missing")
		test.Assert(bytes.Contains(data, []byte(attrPrevData)),
			"Previous xattr missing")
		test.Assert(bytes.Contains(data, []byte(attrData)),
			"Xattr missing")

		// Linked
		dataFile := make([]byte, 100)
		sizeFile, err := syscall.Listxattr(filename, dataFile)
		test.Assert(bytes.Equal(dataFile[:sizeFile], data[:size]),
			"Xattrs aren't shared between links")
	})
}
