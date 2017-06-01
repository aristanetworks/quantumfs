// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// memdb is a stand in memory-based database for use in testing qloggerdb

package qloggerdb


type dataSeries struct {
	seriesNs []uint64
}

type Memdb struct {
	data	map[string]dataSeries
}

func NewMemdb() *Memdb {
	return &Memdb {
		data:	make(map[string]dataSeries, 0),
	}
}

func (db *Memdb) Store(tag string, timeNs uint64) {
	var entry dataSeries
	if oldEntry, exists := db.data[tag]; exists {
		entry = oldEntry
	} else {
		entry.seriesNs = make([]uint64, 0)
	}

	entry.seriesNs = append(entry.seriesNs, timeNs)
	db.data[tag] = entry
}

func (db *Memdb) Fetch(tag string, lastN int) (seriesNs []uint64) {
	if series, exists := db.data[tag]; exists {
		rtn := series.seriesNs
		if len(rtn) > lastN {
			return rtn[len(rtn)-lastN:]
		}

		return rtn
	}

	return nil
}
