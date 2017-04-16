// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "sort"
import "testing"

func TestDirectoryRecordSort(t *testing.T) {
	runTest(t, func(test *testHelper) {
		records := make([]DirectoryRecord, 0)
		dr1 := NewDirectoryRecord()
		dr1.SetFilename("name2")
		records = append(records, dr1)
		dr2 := NewDirectoryRecord()
		dr2.SetFilename("name1")
		records = append(records, dr2)
		dr3 := NewDirectoryRecord()
		dr3.SetFilename("anothername")
		records = append(records, dr3)

		sort.Sort(DirectoryRecordSorterByName(records))
		test.Assert(records[0].Filename() == "anothername",
			"Wrong sort. Found %s expects \"anothername\"",
			records[0].Filename())
		test.Assert(records[1].Filename() == "name1",
			"Wrong sort. Found %s expects \"name1\"",
			records[1].Filename())
		test.Assert(records[2].Filename() == "name2",
			"Wrong sort. Found %s expects \"name2\"",
			records[2].Filename())
	})
}
