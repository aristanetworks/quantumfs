// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "testing"

func TestDirectoryRecordSort(t *testing.T) {
	runTest(t, func(test *testHelper) {
		_, dirEntry := NewDirectoryEntry(4)
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

func TestCacheSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// Measure the cache block size of directory entries
		remain, dirEntry := NewDirectoryEntry(4)
		cacheNum := dirEntry.dir.Entries().Len()
		test.Assert(remain == 0 && cacheNum == 4, "Incorrect size of the "+
			"remain: %d != 0 and of the cache: %d != 4",
			remain, cacheNum)

		// Measure the cache block size of very large files
		vlf := NewVeryLargeFile(5)
		cacheNum = vlf.vlf.LargeFileKeys().Len()
		test.Assert(cacheNum == 5, "Incorrect size of the cache: %d != 5",
			cacheNum)

		// Measure the cache block size of Extended Attributes
		remain, hlEntry := NewHardlinkEntry(6)
		cacheNum = hlEntry.entry.Entries().Len()
		test.Assert(remain == 0 && cacheNum == 6, "Incorrect size of the "+
			"remain: %d != 0 and of the cache: %d != 4",
			remain, cacheNum)
	})
}
