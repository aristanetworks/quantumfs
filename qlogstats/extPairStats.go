// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// extPairStats is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qlogstats

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type request struct {
	lastUpdateGeneration uint64
	time                 int64
}

type extPairStats struct {
	StatExtractorBaseWithGC

	fmtStart string
	fmtStop  string

	requests map[uint64]request
	stats    basicStats
}

func NewExtPairStats(start string, stop string, nametag string) StatExtractor {
	ext := &extPairStats{
		fmtStart: start + "\n",
		fmtStop:  stop + "\n",
		requests: make(map[uint64]request),
	}

	ext.StatExtractorBaseWithGC = NewStatExtractorBaseWithGC(nametag, ext,
		OnFormat, []string{ext.fmtStart, ext.fmtStop})

	ext.run()

	return ext
}

func (ext *extPairStats) process(msg *qlog.LogOutput) {
	if msg.Format == ext.fmtStart {
		ext.startRequest(msg)
	} else if msg.Format == ext.fmtStop {
		ext.stopRequest(msg)
	}
}

func (ext *extPairStats) startRequest(log *qlog.LogOutput) {
	if previous, exists := ext.requests[log.ReqId]; exists {
		fmt.Printf("%s: nested start %d at %d and %d\n",
			ext.Name, log.ReqId, previous.time, log.T)
	}

	ext.requests[log.ReqId] = request{
		lastUpdateGeneration: ext.CurrentGeneration,
		time:                 log.T,
	}
}

func (ext *extPairStats) stopRequest(log *qlog.LogOutput) {
	start, exists := ext.requests[log.ReqId]
	if !exists {
		fmt.Printf("%s: end without start '%s' at %d\n", ext.Name,
			log.Format, log.T)
		return
	}

	delta := log.T - start.time
	if delta < 0 {
		fmt.Printf("Negative delta (%d): |%s| to |%s|\n",
			delta, ext.fmtStart, ext.fmtStop)
	} else {
		ext.stats.NewPoint(int64(delta))
	}
	delete(ext.requests, log.ReqId)
}

func (ext *extPairStats) publish() []Measurement {
	tags := make([]quantumfs.Tag, 0)
	tags = appendNewTag(tags, "statName", ext.Name)

	fields := make([]quantumfs.Field, 0)

	fields = appendNewFieldInt(fields, "average_ns", ext.stats.Average())
	fields = appendNewFieldInt(fields, "maximum_ns", ext.stats.Max())
	fields = appendNewFieldInt(fields, "samples", ext.stats.Count())

	for name, data := range ext.stats.Percentiles() {
		fields = appendNewFieldInt(fields, name, data)
	}

	ext.stats = basicStats{}
	return []Measurement{{
		name:   "quantumFsLatency",
		tags:   tags,
		fields: fields,
	}}
}

func (ext *extPairStats) gc() {
	for reqId, request := range ext.requests {
		if ext.AgedOut(request.lastUpdateGeneration) {
			fmt.Printf("%s: Deleting stale request %d (%d/%d)\n",
				ext.Name, reqId, request.lastUpdateGeneration,
				ext.CurrentGeneration)
			delete(ext.requests, reqId)
		}
	}
}
