// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPointStats is a stat extractor that watches a singular log

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
)

type extPointStats struct {
	format string
	name   string

	stats basicStats
}

func NewExtPointStats(format_ string, nametag string) *extPointStats {
	return &extPointStats{
		format: format_ + "\n",
		name:   nametag,
	}
}

func (ext *extPointStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.format)
	return rtn
}

func (ext *extPointStats) ProcessRequest(request []indentedLog) {
	for _, v := range request {
		ext.stats.NewPoint(int64(v.log.T))
	}
}

func (ext *extPointStats) Publish() (measurement string, tags []quantumfs.Tag,
	fields []quantumfs.Field) {

	tags = make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("statName", ext.name))

	fields = make([]quantumfs.Field, 0)

	fields = append(fields, quantumfs.NewField("samples", ext.stats.Count()))

	ext.stats = basicStats{}
	return "quantumFsPointCount", tags, fields
}
