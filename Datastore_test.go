// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "github.com/aristanetworks/quantumfs/testutils"

func TestDirectoryRecordSort(t *testing.T) {

	helper := &testutils.TestHelper{}
	// test if quantumfs.SortDirectoryRecordsByName()
	// sorts the records based on lexical
	// order of file names
	records := make([]quantumfs.DirectoryRecord, 0)
	dr1 := quantumfs.NewDirectoryRecord()
	dr1.SetFileName("name2")
	records = append(records, dr1)
	dr2 := quantumfs.NewDirectoryRecord()
	dr2.SetFileName("name1")
	records = append(records, dr2)
	dr3 := quantumfs.NewDirectoryRecord()
	dr3.SetFileName("anotherName")
	records = append(records, dr3)

	quantumfs.SortDirectoryRecordsByName(records)
	test.Assert(records[0].Filename() == "anothername",
		"Wrong sort. Found %s expects \"anothername\"",
		records[0].Filename())
	test.Assert(records[1].Filename() == "name1",
		"Wrong sort. Found %s expects \"anothername\"",
		records[1].Filename())
	test.Assert(records[2].Filename() == "name2",
		"Wrong sort. Found %s expects \"anothername\"",
		records[2].Filename())
}
