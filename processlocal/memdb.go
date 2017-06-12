// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// memdb is a stand in memory-based database for use in testing qloggerdb

package processlocal

import (
	"github.com/aristanetworks/quantumfs/loggerdb"
)

type dataSeries struct {
	tags   map[string]string
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
	tagMap := make(map[string]string)
	// Use a set to make Fetch faster, but use maps because Golang has no sets
	for _, tag := range tags_ {
		tagMap[tag.Name] = tag.Data
	}

	db.data = append(db.data, dataSeries{
		tags:   tagMap,
		fields: fields_,
	})
}

func (db *Memdb) Fetch(withTags []qloggerdb.Tag, field string, lastN int) []uint64 {
	rtn := make([]uint64, 0)

	for _, entry := range db.data {
		// check if the data has all the tags we need
		outputData := true
		for _, needTag := range withTags {
			// missing a tag, so we don't care about this data point
			if tagData, exists := entry.tags[needTag.Name]; !exists ||
				tagData != needTag.Data {

				outputData = false
				break
			}
		}

		if outputData {
			// add the field, if it exists
			for _, hasField := range entry.fields {
				if hasField.Name == field {
					rtn = append(rtn, hasField.Data)
					break
				}
			}
		}
	}

	return rtn
}
