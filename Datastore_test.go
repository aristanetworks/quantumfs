// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

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

		dirEntry.SortByName()

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
