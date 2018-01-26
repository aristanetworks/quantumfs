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
	Fields      map[string]interface{}
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

	fieldMap := make(map[string]interface{})
	for _, field := range fields_ {
		fieldMap[field.Name] = field.Data
	}

	db.Data = append(db.Data, dataSeries{
		Measurement: measurement,
		Tags:        tagMap,
		Fields:      fieldMap,
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

		datum, exists := entry.Fields[field]
		if outputData && exists {
			rtn = append(rtn, datum)
		}
	}

	return rtn
}
