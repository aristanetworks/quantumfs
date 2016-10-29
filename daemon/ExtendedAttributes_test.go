// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on extended attributes

import "bytes"
import "encoding/binary"
import "fmt"
import "os"
import "strings"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"

func TestExtendedAttrReadWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		attrNoData := "user.nodata"
		attrData := "user.data"
		attrDataData := []byte("extendedattributedata")

		err = syscall.Setxattr(testFilename, attrNoData, []byte{}, 0)
		test.assert(err == nil, "Error setting nodata XAttr: %v", err)

		err = syscall.Setxattr(testFilename, attrData, attrDataData, 0)
		test.assert(err == nil, "Error setting data XAttr: %v", err)

		// Now confirm we can read them out again
		data := make([]byte, 100)

		size, err := syscall.Getxattr(testFilename, attrNoData, data)
		test.assert(err == nil, "Error reading nodata XAttr: %v", err)
		test.assert(size == 0, "nodata XAttr size not zero: %d", size)

		size, err = syscall.Getxattr(testFilename, attrData, data)
		test.assert(err == nil, "Error reading data XAttr: %v", err)
		test.assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data,
			attrDataData)

		// Finally confirm we can overwrite an attribute and read it back
		data = make([]byte, 100)
		attrDataData = []byte("extendedattributedata2")
		err = syscall.Setxattr(testFilename, attrData, attrDataData, 0)
		test.assert(err == nil, "Error setting data XAttr: %v", err)

		size, err = syscall.Getxattr(testFilename, attrData, data)
		test.assert(err == nil, "Error reading data XAttr: %v", err)
		test.assert(size == len(attrDataData),
			"data XAttr size incorrect: %d", size)
		test.assert(bytes.Equal(data[:size], attrDataData),
			"Didn't get the same data back '%s' '%s'", data,
			attrDataData)
	})
}

func TestExtendedAttrList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		// Confirm no attributes
		data := make([]byte, 64000)
		size, err := syscall.Listxattr(testFilename, data)
		test.assert(err == nil, "Error listing XAttr: %v", err)
		test.assert(size == 0, "Unexpected XAttr, size %d", size)

		// Add a bunch of attributes
		const N = 100
		for i := 0; i < N; i++ {
			attrName := fmt.Sprintf("user.attr%d", i)
			err = syscall.Setxattr(testFilename, attrName, []byte{}, 0)
			test.assert(err == nil, "Error setting XAttr %s: %v",
				attrName, err)
		}

		// Confirm they are all there
		data = make([]byte, 64000)
		size, err = syscall.Listxattr(testFilename, data)
		test.assert(err == nil, "Error listing XAttr: %v", err)
		test.assert(size > 0, "Expected XAttr, but didn't find any")
		test.assert(size <= len(data), "XAttr names overflowed buffer")

		// Remove all the cases of the virtual extended attribute:
		// typeKey
		data = data[:size]
		names := bytes.Split(data, []byte("\x00"))
		names = names[:len(names)-1] // Remove empty last element
		test.assert(len(names) == N+1, "Fewer XAttr than expected: %d != %d",
			len(names), N+1)

		for _, nameBytes := range names {
			name := string(nameBytes)
			if name == XAttrTypeKey {
				continue
			}
			test.assert(strings.HasPrefix(name, "user.attr"),
				"Incorrect XAttr name %s", name)
		}
	})
}

func TestExtendedAttrReadNonExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		data := make([]byte, 100)

		size, err := syscall.Getxattr(testFilename, "user.does_not_exist",
			data)
		test.assert(err != nil, "Expected error reading nodata XAttr")
		test.assert(strings.Contains(err.Error(), "no data available"),
			"Expected no data available: %v", err)
		test.assert(size <= 0, "Non-zero size with non-existant XAttr: %d",
			size)
	})
}

func TestExtendedAttrRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating test file: %v", err)
		fd.Close()

		verify := func(N int) {
			// Confirm they are all there
			data := make([]byte, 64000)
			size, err := syscall.Listxattr(testFilename, data)
			test.assert(err == nil, "Error listing XAttr: %v", err)
			test.assert(size > 0, "Expected XAttr, but didn't find any")
			test.assert(size <= len(data),
				"XAttr names overflowed buffer")

			// Remove all the cases of the virtual extended attribute:
			// typeKey
			data = data[:size]
			names := bytes.Split(data, []byte("\x00"))
			names = names[:len(names)-1] // Remove empty last element
			test.assert(len(names) == N+1,
				"Fewer XAttr than expected: %d != %d",
				len(names), N+1)

			nameSet := make(map[string]bool)
			for _, nameBytes := range names {
				name := string(nameBytes)
				if name == XAttrTypeKey {
					continue
				}
				test.assert(strings.HasPrefix(name, "user.attr"),
					"Incorrect XAttr name %s", name)
				nameSet[name] = true
			}
			// Ensure we have no duplicates
			test.assert(len(nameSet) == N, "Duplicated names retrieved")
		}

		// Add a bunch of attributes
		N := 100
		for i := 0; i < N; i++ {
			attrName := fmt.Sprintf("user.attr%d", i)
			err = syscall.Setxattr(testFilename, attrName, []byte{}, 0)
			test.assert(err == nil, "Error setting XAttr %s: %v",
				attrName, err)
		}

		// Delete a non-existent attribute
		err = syscall.Removexattr(testFilename, "user.does_not_exist")
		test.assert(err != nil,
			"Expected error when deleting non-existent XAttr")
		test.assert(strings.Contains(err.Error(), "no data available"),
			"Expected error 'no data available': %s", err.Error())

		// Delete an attribute from the middle
		err = syscall.Removexattr(testFilename, "user.attr50")
		test.assert(err == nil, "Error when removing XAttr from middle: %v",
			err)
		N--
		verify(N)

		// Delete an attribute from the end
		err = syscall.Removexattr(testFilename, "user.attr99")
		test.assert(err == nil, "Error when removing XAttr from end: %v",
			err)
		N--
		verify(N)
	})
}

// Verify the get XAttr function for the self-defined Extended Attributes: typeKey
func TestXAttrTypeKeyGet(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()

		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.assert(err == nil, "Error creating a small file: %v", err)

		dirName := workspace + "/dir"
		err = syscall.Mkdir(dirName, 0124)
		test.assert(err == nil, "Error creating a directory: %v", err)

		linkName := workspace + "/link"
		err = syscall.Symlink(testFilename, linkName)
		test.assert(err == nil, "Error creating a symlink: %v", err)

		// sync the workspace
		test.syncAllWorkspaces()

		dst := make([]byte, TypeKeyLength)

		// check the non-existing file
		nonExist := workspace + "/noExist"
		sz, err := syscall.Getxattr(nonExist, XAttrTypeKey, dst)
		test.assert(err != nil,
			"Error getting XAttr from a non-existing file")

		// check the file
		sz, err = syscall.Getxattr(testFilename, XAttrTypeKey, dst)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the file typeKey: %v with a size of %d",
			err, sz)

		type_ := dst[TypeKeyLength-9]
		key := dst[:TypeKeyLength-9]
		size := binary.LittleEndian.Uint64(dst[TypeKeyLength-8:])

		// Extract the internal ObjectKey from QuantumFS
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ind test file: %v", err)
		var id InodeId
		id = InodeId(stat.Ino)
		inode := test.qfs.inodes[id]
		record, err := inode.parent().getChildRecord(&test.qfs.c, id)

		// Verify the type and key matching
		test.assert(type_ == quantumfs.ObjectTypeSmallFile &&
			size == record.Size() &&
			bytes.Equal(key, record.ID().Value()),
			"Error getting the file typeKey: %v with %d, keys of %v-%v",
			err, type_, key, record.ID().Value())

		// check the directory
		sz, err = syscall.Getxattr(dirName, XAttrTypeKey, dst)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the directory typeKey: %v with a size of %d",
			err, sz)

		type_ = dst[TypeKeyLength-9]
		key = dst[:TypeKeyLength-9]
		size = binary.LittleEndian.Uint64(dst[TypeKeyLength-8:])

		// Extract the internal ObjectKey from QuantumFS
		err = syscall.Stat(dirName, &stat)
		test.assert(err == nil, "Error stat'ind directory: %v", err)
		id = InodeId(stat.Ino)
		inode = test.qfs.inodes[id]
		record, err = inode.parent().getChildRecord(&test.qfs.c, id)

		// Verify the type and key matching
		test.assert(type_ == quantumfs.ObjectTypeDirectoryEntry &&
			size == record.Size() &&
			bytes.Equal(key, record.ID().Value()),
			"Error getting the dir typeKey: %v with %d, keys of %v-%v",
			err, key, type_, record.ID().Value())

		// check the symlink
		sz, err, dst = lGetXattr(linkName, XAttrTypeKey, TypeKeyLength)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the directory typeKey: %v with a size of %d",
			err, sz)

		type_ = dst[TypeKeyLength-9]
		key = dst[:TypeKeyLength-9]
		size = binary.LittleEndian.Uint64(dst[TypeKeyLength-8:])

		// Extract the internal ObjectKey from QuantumFS
		err = syscall.Lstat(linkName, &stat)
		test.assert(err == nil, "Error stat'ind symlink: %v", err)
		id = InodeId(stat.Ino)
		inode = test.qfs.inodes[id]
		record, err = inode.parent().getChildRecord(&test.qfs.c, id)

		// Verify the type and key matching
		test.assert(type_ == quantumfs.ObjectTypeSymlink &&
			size == record.Size() &&
			bytes.Equal(key, record.ID().Value()),
			"Error getting the link typeKey: %v with %d, keys of %v-%v",
			err, type_, key, record.ID().Value(), size, record.Size())

	})
}

// Verify the set/remove XAttr function for typeKey is illegal
func TestXAttrTypeKeySetRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()

		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.assert(err == nil, "Error creating a small file: %v", err)

		// Reset the typeKey
		data := []byte("1234567890abcdefghijk700000008")
		err = syscall.Setxattr(testFilename, XAttrTypeKey, data, 0)
		test.assert(err != nil,
			"Error finishing illegal SetXAttr: %v", err)

		// Remove the typeKey
		err = syscall.Removexattr(testFilename, XAttrTypeKey)
		test.assert(err != nil,
			"Error finishing illegal RemoveXAttr: %v", err)
	})
}

// Verify list XAttr function will attach typeKey behind the real extended attributes
func TestXAttrTypeKeyList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()

		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.assert(err == nil, "Error creating a small file: %v", err)

		data := []byte("TestOne")
		data2 := []byte("TestTwo")

		err = syscall.Setxattr(testFilename, "security.one", data, 0)
		test.assert(err == nil, "Error setting XAttr: %v", err)

		err = syscall.Setxattr(testFilename, "security.two", data2, 0)
		test.assert(err == nil, "Error setting XAttr: %v", err)

		dst := make([]byte, 64)
		_, err = syscall.Listxattr(testFilename, dst)
		test.assert(err == nil &&
			bytes.Contains(dst, []byte("security.one")) &&
			bytes.Contains(dst, []byte("security.two")) &&
			bytes.Contains(dst, []byte(XAttrTypeKey)),
			"Error listing XAttr: %v with content of %s",
			err, dst)
	})
}
