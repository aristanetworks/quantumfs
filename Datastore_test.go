// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "encoding/hex"
import "strings"
import "testing"

func TestDirectoryRecordSort(t *testing.T) {
	runTest(t, func(test *testHelper) {
		dirEntry := NewDirectoryEntry()
		dirEntry.SetNumEntries(3)

		dr1 := NewDirectoryRecord()
		dr1.SetFilename("name2")
		dirEntry.SetEntry(0, dr1)
		dr2 := NewDirectoryRecord()
		dr2.SetFilename("name1")
		dirEntry.SetEntry(1, dr2)
		dr3 := NewDirectoryRecord()
		dr3.SetFilename("anothername")
		dirEntry.SetEntry(2, dr3)

		dirEntry.SortRecordsByName()

		test.Assert(dirEntry.Entry(0).Filename() == "anothername",
			"Wrong sort. Found %s expects \"anothername\"",
			dirEntry.Entry(0).Filename())
		test.Assert(dirEntry.Entry(1).Filename() == "name1",
			"Wrong sort. Found %s expects \"name1\"",
			dirEntry.Entry(1).Filename())
		test.Assert(dirEntry.Entry(2).Filename() == "name2",
			"Wrong sort. Found %s expects \"name2\"",
			dirEntry.Entry(2).Filename())
	})
}

func TestEmptyKeyToString(t *testing.T) {
	runTest(t, func(test *testHelper) {

		key := EmptyDirKey
		parts := strings.Split(strings.Trim(key.Text(), "()"), " ")
		text := parts[1]
		key2, err := FromText(text)
		test.Assert(err == nil, "error in FromText() err: %v", err)
		test.Assert(key.IsEqualTo(key2),
			"The key before and after are not the same")
	})
}
func TestKeyToString(t *testing.T) {
	runTest(t, func(test *testHelper) {

		// With an arbirary key
		bytes, err := hex.DecodeString(
			"032ee5784d3bd45789521abcdebbc45437c890fce8")
		test.Assert(err == nil, "error in hex.DecodeString() err: %v", err)
		key1 := NewObjectKeyFromBytes(bytes)

		parts := strings.Split(strings.Trim(key1.Text(), "()"), " ")
		text := parts[1]
		key2, err := FromText(text)
		test.Assert(err == nil, "error in FromText() err: %v", err)
		test.Assert(key1.IsEqualTo(key2),
			"The key before and after are not the same")
	})
}
