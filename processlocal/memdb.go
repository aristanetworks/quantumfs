// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// memdb is a stand in memory-based database for use in testing qloggerdb

package processlocal

import (
	"github.com/aristanetworks/quantumfs/loggerdb"
)

type dataSeries struct {
	tags   []qloggerdb.Tag
	fields []qloggerdb.Field
}

type Memdb struct {
	data []dataSeries
}

func NewMemdb() *Memdb {
	return &Memdb{
		data: make([]dataSeries, 0),
	}
}

func (db *Memdb) Store(tags_ []qloggerdb.Tag, fields_ []qloggerdb.Field) {
	db.data = append(db.data, dataSeries{
		tags:   tags_,
		fields: fields_,
	})
}

func (db *Memdb) Fetch(withTags []qloggerdb.Tag, field string, lastN int) []uint64 {
	rtn := make([]uint64, 0)
	for _, i := range db.data {
		// check if the data has all the tags we need
		outputData := true
		for _, needTag := range withTags {
			foundTag := false
			for _, haveTag := range i.tags {
				if haveTag == needTag {
					foundTag = true
					break
				}
			}

			// missing a tag, so we don't have about this data point
			if !foundTag {
				outputData = false
				break
			}
		}

		if outputData {
			// add the field, if it exists
			for _, hasField := range i.fields {
				if hasField.Name == field {
					rtn = append(rtn, hasField.Data)
					break
				}
			}
		}
	}

	return rtn
}
