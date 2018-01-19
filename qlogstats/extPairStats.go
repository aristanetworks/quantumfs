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
	time                 int64
}

type extPairStats struct {
	fmtStart string
	fmtStop  string
	name     string
	messages chan *qlog.LogOutput

	// These channels are used instead of a mutex to pause the processing thread
	// so publishing or GC can occur. This eliminates the mutex overhead from the
	// common mutex processing path.
	pause   chan struct{}
	paused  chan struct{}
	unpause chan struct{}

	lock              utils.DeferableMutex
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
		pause:    make(chan struct{}),
		paused:   make(chan struct{}),
		unpause:  make(chan struct{}),
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
		select {
		case log := <-ext.messages:
			if log.Format == ext.fmtStart {
				ext.startRequest(log)
			} else if log.Format == ext.fmtStop {
				ext.stopRequest(log)
			}
		case <-ext.pause:
			// Notify the other goroutine that we are paused
			ext.paused <- struct{}{}
			// Wait until they are complete
			<-ext.unpause
		}
	}
}

func (ext *extPairStats) startRequest(log *qlog.LogOutput) {
	if previous, exists := ext.requests[log.ReqId]; exists {
		fmt.Printf("%s: nested start %d at %d and %d\n",
			ext.name, log.ReqId, previous.time, log.T)
	}

	ext.requests[log.ReqId] = request{
		lastUpdateGeneration: ext.currentGeneration,
		time:                 log.T,
	}
}

func (ext *extPairStats) stopRequest(log *qlog.LogOutput) {
	start, exists := ext.requests[log.ReqId]
	if !exists {
		fmt.Printf("%s: end without start '%s' at %d\n", ext.name,
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

func (ext *extPairStats) Publish() []Measurement {

	defer ext.lock.Lock().Unlock()
	ext.pause <- struct{}{}
	<-ext.paused
	defer func() {
		ext.unpause <- struct{}{}
	}()

	tags := make([]quantumfs.Tag, 0)
	tags = appendNewTag(tags, "statName", ext.name)

	fields := make([]quantumfs.Field, 0)

	fields = appendNewField(fields, "average_ns", ext.stats.Average())
	fields = appendNewField(fields, "maximum_ns", ext.stats.Max())
	fields = appendNewField(fields, "samples", ext.stats.Count())

	for name, data := range ext.stats.Percentiles() {
		fields = appendNewField(fields, name, data)
	}

	ext.stats = basicStats{}
	return []Measurement{{
		name:   "quantumFsLatency",
		tags:   tags,
		fields: fields,
	}}
}

func (ext *extPairStats) GC() {
	defer ext.lock.Lock().Unlock()
	ext.pause <- struct{}{}
	<-ext.paused
	defer func() {
		ext.unpause <- struct{}{}
	}()

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
