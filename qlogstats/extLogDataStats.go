// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extLogDataStats is a stat extractor that produces a histogram from log data

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extLogDataStats struct {
	StatExtractorBase

	dataFetch func(*qlog.LogOutput) (int64, bool)
	errors    int64

	stats histogram
}

func NewHistogramExtractor(format string, nametag string, min int64, max int64,
	buckets int64, normalize bool, paramIdx int) StatExtractor {

	return NewExtLogDataStats(format, nametag, NewHistogram(min, max, buckets,
		normalize), GetParamIntFn(paramIdx))
}

func NewExtLogDataStats(format string, nametag string, histo histogram,
	fetchFn func(*qlog.LogOutput) (int64, bool)) StatExtractor {

	ext := &extLogDataStats{
		dataFetch: fetchFn,
		stats:     histo,
	}
	ext.StatExtractorBase = NewStatExtractorBase(nametag, ext, OnFormat,
		[]string{format + "\n"})

	ext.run()

	return ext
}

func (ext *extLogDataStats) process(msg *qlog.LogOutput) {
	data, success := ext.dataFetch(msg)
	if !success {
		ext.errors++
	} else {
		ext.stats.NewPoint(data)
	}
}

func (ext *extLogDataStats) publish() []Measurement {
	tags := make([]quantumfs.Tag, 0)
	tags = appendNewTag(tags, "statName", ext.Name)

	fields := make([]quantumfs.Field, 0)

	fields = appendNewFieldInt(fields, "samples", ext.stats.Count())

	for name, data := range ext.stats.Histogram() {
		fields = appendNewFieldInt(fields, name, data)
	}

	// Take note of any parsing errors
	fields = appendNewFieldInt(fields, "errors", ext.errors)

	ext.stats.Clear()
	ext.errors = 0
	return []Measurement{{
		name:   "quantumFsLogDataStats",
		tags:   tags,
		fields: fields,
	}}
}

func GetParamIntFn(paramIdx int) func(*qlog.LogOutput) (int64, bool) {
	return func(log *qlog.LogOutput) (int64, bool) {
		if len(log.Args) <= paramIdx {
			return 0, false
		}

		num := log.Args[paramIdx].(int64)
		return num, true
	}
}
