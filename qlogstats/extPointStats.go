// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPointStats is a stat extractor that watches a singular log

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extPointStats struct {
	format        string
	name          string
	messages      chan StatCommand
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
		name:          nametag,
		messages:      make(chan StatCommand, 10000),
		partialFormat: partialFormat,
	}

	go ext.process()

	return ext
}

func (ext *extPointStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.format)
	return rtn
}

func (ext *extPointStats) Chan() chan StatCommand {
	return ext.messages
}

func (ext *extPointStats) Type() TriggerType {
	if ext.partialFormat {
		return OnPartialFormat
	} else {
		return OnFormat
	}
}

func (ext *extPointStats) process() {
	for {
		cmd := <-ext.messages
		switch cmd.Type() {
		case MessageCommandType:
			log := cmd.Data().(*qlog.LogOutput)
			ext.stats.NewPoint(int64(log.T))
		case PublishCommandType:
			resultChannel := cmd.Data().(chan PublishResult)
			resultChannel <- ext.publish()
		case GcCommandType:
			// do nothing since we store no state
		}
	}
}

func (ext *extPointStats) publish() PublishResult {
	tags := make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("statName", ext.name))

	fields := make([]quantumfs.Field, 0)

	fields = append(fields, quantumfs.NewField("samples", ext.stats.Count()))

	ext.stats = basicStats{}
	return PublishResult{
		measurement: "quantumFsPointCount",
		tags:        tags,
		fields:      fields,
	}
}
