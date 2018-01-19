// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPointStats is a stat extractor that watches a singular log

package qlogstats

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type extPointStats struct {
	format        string
	name          string
	messages      chan *qlog.LogOutput
	partialFormat bool

	// These channels are used instead of a mutex to pause the processing thread
	// so publishing or GC can occur. This eliminates the mutex overhead from the
	// common mutex processing path.
	pause   chan struct{}
	paused  chan struct{}
	unpause chan struct{}

	lock  utils.DeferableMutex
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
		messages:      make(chan *qlog.LogOutput, 10000),
		partialFormat: partialFormat,
		pause:         make(chan struct{}),
		paused:        make(chan struct{}),
		unpause:       make(chan struct{}),
	}

	go ext.process()

	return ext
}

func (ext *extPointStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.format)
	return rtn
}

func (ext *extPointStats) Chan() chan *qlog.LogOutput {
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
		select {
		case log := <-ext.messages:
			func() {
				defer ext.lock.Lock().Unlock()
				ext.stats.NewPoint(int64(log.T))
			}()
		case <-ext.pause:
			// Notify the other goroutine that we are paused
			ext.paused <- struct{}{}
			// Wait until they are complete
			<-ext.unpause
		}
	}
}

func (ext *extPointStats) Publish() []Measurement {

	defer ext.lock.Lock().Unlock()
	ext.pause <- struct{}{}
	<-ext.paused
	defer func() {
		ext.unpause <- struct{}{}
	}()

	tags := make([]quantumfs.Tag, 0)
	tags = appendNewTag(tags, "statName", ext.name)

	fields := make([]quantumfs.Field, 0)

	fields = appendNewField(fields, "samples", ext.stats.Count())

	ext.stats = basicStats{}
	return []Measurement{{
		name:   "quantumFsPointCount",
		tags:   tags,
		fields: fields,
	}}
}

func (ext *extPointStats) GC() {
	// We keep no state, so there is nothing to do
}
