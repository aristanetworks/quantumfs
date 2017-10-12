// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPairStats is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qlogstats

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type request struct {
	lastUpdateGeneration uint64
	log                  *qlog.LogOutput
}

type extPairStats struct {
	fmtStart string
	fmtStop  string
	name     string
	messages chan *qlog.LogOutput

	reqLock           utils.DeferableMutex
	requests          map[uint64]request
	currentGeneration uint64

	stats basicStats
}

func NewExtPairStats(start string, stop string, nametag string) StatExtractor {
	ext := &extPairStats{
		fmtStart: start + "\n",
		fmtStop:  stop + "\n",
		name:     nametag,
		messages: make(chan *qlog.LogOutput, 10000),
		requests: make(map[uint64]request),
	}

	go ext.process()

	return ext
}

func (ext *extPairStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.fmtStart)
	rtn = append(rtn, ext.fmtStop)
	return rtn
}

func (ext *extPairStats) Chan() chan *qlog.LogOutput {
	return ext.messages
}

func (ext *extPairStats) Type() TriggerType {
	return OnFormat
}

func (ext *extPairStats) process() {
	for {
		log := <-ext.messages

		if log.Format == ext.fmtStart {
			ext.startRequest(log)
		} else if log.Format == ext.fmtStop {
			ext.stopRequest(log)
		}
	}
}

func (ext *extPairStats) startRequest(log *qlog.LogOutput) {
	defer ext.reqLock.Lock().Unlock()

	if _, exists := ext.requests[log.ReqId]; exists {
		panic("Nested indent not supported")
	}

	ext.requests[log.ReqId] = request{
		lastUpdateGeneration: ext.currentGeneration,
		log:                  log,
	}
}

func (ext *extPairStats) stopRequest(log *qlog.LogOutput) {
	defer ext.reqLock.Lock().Unlock()

	start, exists := ext.requests[log.ReqId]
	if !exists {
		fmt.Printf("%s: end without start '%s' at %d\n", ext.name,
			log.Format, log.T)
		return
	}

	delta := log.T - start.log.T
	if delta < 0 {
		fmt.Printf("Negative delta (%d): |%s| to |%s|\n",
			delta, ext.fmtStart, ext.fmtStop)
	} else {
		ext.stats.NewPoint(int64(delta))
	}
	delete(ext.requests, log.ReqId)
}

func (ext *extPairStats) Publish() (measurement string, tags []quantumfs.Tag,
	fields []quantumfs.Field) {

	tags = make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("statName", ext.name))

	fields = make([]quantumfs.Field, 0)

	fields = append(fields, quantumfs.NewField("average_ns",
		ext.stats.Average()))
	fields = append(fields, quantumfs.NewField("maximum_ns", ext.stats.Max()))
	fields = append(fields, quantumfs.NewField("samples", ext.stats.Count()))

	for name, data := range ext.stats.Percentiles() {
		fields = append(fields, quantumfs.NewField(name, data))
	}

	ext.stats = basicStats{}
	return "quantumFsLatency", tags, fields
}

func (ext *extPairStats) GC() {
	defer ext.reqLock.Lock().Unlock()

	ext.currentGeneration++

	for reqId, request := range ext.requests {
		if request.lastUpdateGeneration+2 < ext.currentGeneration {
			fmt.Printf("%s: Deleting stale request %d (%d/%d)\n",
				ext.name, reqId, request.lastUpdateGeneration,
				ext.currentGeneration)
			delete(ext.requests, reqId)
		}
	}
}
