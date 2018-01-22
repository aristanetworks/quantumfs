// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extLogDataStats is a stat extractor that produces a histogram from log data

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extLogDataStats struct {
	format	string
	name	string
	messages	chan StatCommand
	dataFetch	func (*qlog.LogOutput) (int64, bool)
	errors	int64

	stats histoStats
}

func NewExtLogDataStats(format string, nametag string, histo histoStats,
	fetchFn func(*qlog.LogOutput) (int64, bool)) StatExtractor {

	ext := &extLogDataStats {
		format:	format,
		name:	nametag,
		messages:	make(chan StatCommand, 10000),
		dataFetch:	fetchFn,
		stats:		histo,
	}

	go ext.process()

	return ext
}

func (ext *extLogDataStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.format)
	return rtn
}

func (ext *extLogDataStats) Chan() chan StatCommand {
	return ext.messages
}

func (ext *extLogDataStats) Type() TriggerType {
	return OnFormat
}

func (ext *extLogDataStats) process() {
	for {
		cmd := <-ext.messages
		switch cmd.Type() {
		case MessageCommandType:
			log := cmd.Data().(*qlog.LogOutput)
			data, success := ext.dataFetch(log)
			if !success {
				ext.errors++
			} else {
				ext.stats.NewPoint(data)
			}
		case PublishCommandType:
			resultChannel := cmd.Data().(chan PublishResult)
			resultChannel <- ext.publish()
		case GcCommandType:
			// do nothing since we clear every publish
		}
	}
}

func (ext *extLogDataStats) publish() PublishResult {
	tags := make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("statName", ext.name))

	fields := make([]quantumfs.Field, 0)

	fields = append(fields, quantumfs.NewField("samples", ext.stats.Count()))
	
	for name, data := range ext.stats.Histogram() {
		fields = append(fields, quantumfs.NewField(name, data))
	}

	// Take note of any parsing errors
	fields = append(fields, quantumfs.NewField("errors", ext.errors))

	ext.stats.Clear()
	ext.errors = 0
	return PublishResult{
		measurement: "quantumFsLogDataStats",
		tags:        tags,
		fields:      fields,
	}
}
