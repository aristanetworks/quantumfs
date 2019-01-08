// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// extPointStats is a stat extractor that watches a singular log

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extPointStats struct {
	StatExtractorBase

	partialFormat bool

	stats basicStats
}

func NewExtPointStats(format string, nametag string) StatExtractor {
	return newExtPointStats(format, nametag, false)
}

func NewExtPointStatsPartialFormat(format string, nametag string) StatExtractor {
	return newExtPointStats(format, nametag, true)
}

func newExtPointStats(format string, nametag string,
	partialFormat bool) StatExtractor {

	if !partialFormat {
		format = format + "\n"
	}

	ext := &extPointStats{
		partialFormat: partialFormat,
	}

	type_ := OnFormat
	if partialFormat {
		type_ = OnPartialFormat
	}
	ext.StatExtractorBase = NewStatExtractorBase(nametag, ext, type_,
		[]string{format})

	ext.run()

	return ext
}

func (ext *extPointStats) process(msg *qlog.LogOutput) {
	ext.stats.NewPoint(int64(msg.T))
}

func (ext *extPointStats) publish() []Measurement {
	tags := make([]quantumfs.Tag, 0)
	tags = appendNewTag(tags, "statName", ext.Name)

	fields := make([]quantumfs.Field, 0)

	fields = appendNewFieldInt(fields, "samples", ext.stats.Count())

	ext.stats = basicStats{}
	return []Measurement{{
		name:   "quantumFsPointCount",
		tags:   tags,
		fields: fields,
	}}
}
