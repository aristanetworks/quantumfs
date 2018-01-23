// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPointStats is a stat extractor that watches a singular log

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extPointStats struct {
	StatExtractorBase

	format        string
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
		format:        format,
		partialFormat: partialFormat,
	}

	ext.StatExtractorBase = NewStatExtractorBase(nametag, ext)

	ext.run()

	return ext
}

func (ext *extPointStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.format)
	return rtn
}

func (ext *extPointStats) Type() TriggerType {
	if ext.partialFormat {
		return OnPartialFormat
	} else {
		return OnFormat
	}
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
