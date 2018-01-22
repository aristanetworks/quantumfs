// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// memdb is a stand in memory-based database for use in timeseriesdb

package processlocal

import (
	"github.com/aristanetworks/quantumfs"
)

type dataSeries struct {
	Measurement string
	Tags        map[string]string
	Fields      []quantumfs.Field
}

type Memdb struct {
	Data []dataSeries
}

func NewMemdb(conf string) quantumfs.TimeSeriesDB {
	return &Memdb{
		Data: make([]dataSeries, 0),
	}
}

func (db *Memdb) Store(measurement string, tags_ []quantumfs.Tag,
	fields_ []quantumfs.Field) {

	tagMap := make(map[string]string)
	// Use a set to make Fetch faster, but use maps because Golang has no sets
	for _, tag := range tags_ {
		tagMap[tag.Name] = tag.Data
	}

	db.Data = append(db.Data, dataSeries{
		Measurement: measurement,
		Tags:        tagMap,
		Fields:      fields_,
	})
}

func (db *Memdb) Fetch(withTags []quantumfs.Tag, field string,
	lastN int) []interface{} {

	rtn := make([]interface{}, 0)

	for _, entry := range db.Data {
		// check if the data has all the tags we need
		outputData := true
		for _, needTag := range withTags {
			// missing a tag, so we don't care about this data point
			if tagData, exists := entry.Tags[needTag.Name]; !exists ||
				tagData != needTag.Data {

				outputData = false
				break
			}
		}

		if outputData {
			// add the field, if it exists
			for _, hasField := range entry.Fields {
				if hasField.Name == field {
					rtn = append(rtn, hasField.Data)
					break
				}
			}
		}
	}

	return rtn
}
