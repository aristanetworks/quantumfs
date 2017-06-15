// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"container/list"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type logTrack struct {
	logs        qlog.LogStack
	listElement *list.Element
}

type trackerKey struct {
	reqId       uint64
	lastLogTime time.Time
}

type StatExtractor interface {
	ProcessRequest(request qlog.LogStack)

	Publish() ([]quantumfs.Tag, []quantumfs.Field)
}

type StatExtractorConfig struct {
	extractor  StatExtractor
	statPeriod time.Duration
	lastOutput time.Time
}

func NewStatExtractorConfig(ext StatExtractor,
	period time.Duration) StatExtractorConfig {

	return StatExtractorConfig{
		extractor:  ext,
		statPeriod: period,
	}
}

func AggregateLogs(mode qlog.LogProcessMode, filename string,
	db quantumfs.TimeSeriesDB, extractors []StatExtractorConfig) *Aggregator {

	reader := qlog.NewReader(filename)
	agg := NewAggregator(db, extractors)

	reader.ProcessLogs(mode, func(v qlog.LogOutput) {
		agg.ProcessLog(v)
	})

	return agg
}

type Aggregator struct {
	db            quantumfs.TimeSeriesDB
	logsByRequest map[uint64]logTrack

	// track the oldest untouched requests so we can push them to the stat
	// extractors after the resting period (so we're confident there are no
	// more logs coming for each request)
	requestSequence list.List

	statExtractors  []StatExtractorConfig
	RequestEndAfter time.Duration

	queueMutex utils.DeferableMutex
	queueLogs  []qlog.LogOutput
}

func NewAggregator(db_ quantumfs.TimeSeriesDB,
	extractors []StatExtractorConfig) *Aggregator {

	rtn := Aggregator{
		db:              db_,
		logsByRequest:   make(map[uint64]logTrack),
		statExtractors:  extractors,
		RequestEndAfter: time.Second * 30,
		queueLogs:       make([]qlog.LogOutput, 0),
	}

	// Sync all extractors
	now := time.Now()
	for i, v := range rtn.statExtractors {
		v.lastOutput = now
		rtn.statExtractors[i] = v
	}

	go rtn.ProcessThread()

	return &rtn
}

func (agg *Aggregator) ProcessThread() {
	for {
		logs := func() []qlog.LogOutput {
			defer agg.queueMutex.Lock().Unlock()

			// nothing to do
			if len(agg.queueLogs) == 0 {
				return []qlog.LogOutput{}
			}

			// Take a small performance hit in creating a new array,
			// but gain a much quicker mutex unlock
			rtn := agg.queueLogs
			agg.queueLogs = make([]qlog.LogOutput, 0)
			return rtn
		}()

		for _, log := range logs {
			agg.processLog(log)
		}

		// Now check if any requests are old and ready to go to extractors
		now := time.Now()
		for {
			requestElem := agg.requestSequence.Front()

			if requestElem == nil {
				break
			}

			request := requestElem.Value.(trackerKey)
			if now.Sub(request.lastLogTime) > time.Duration(0+
				agg.RequestEndAfter) {

				agg.requestSequence.Remove(requestElem)
				reqLogs := agg.logsByRequest[request.reqId]
				delete(agg.logsByRequest, request.reqId)

				for _, e := range agg.statExtractors {
					e.extractor.ProcessRequest(reqLogs.logs)
				}
			} else {
				// No more requests ready to extract stats
				break
			}
		}

		// Lastly check if any extractors need to Publish
		for i, extractor := range agg.statExtractors {
			if now.Sub(extractor.lastOutput) > extractor.statPeriod {
				tags, fields := extractor.extractor.Publish()
				if tags != nil && len(tags) > 0 {
					agg.db.Store(tags, fields)
				}

				extractor.lastOutput = now
				agg.statExtractors[i] = extractor
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (agg *Aggregator) ProcessLog(v qlog.LogOutput) {
	defer agg.queueMutex.Lock().Unlock()

	agg.queueLogs = append(agg.queueLogs, v)
}

func (agg *Aggregator) processLog(v qlog.LogOutput) {
	var tracker logTrack
	if tracker_, exists := agg.logsByRequest[v.ReqId]; exists {
		tracker = tracker_
		agg.requestSequence.MoveToBack(tracker.listElement)
	} else {
		tracker.logs = make([]qlog.LogOutput, 0)
		newElem := agg.requestSequence.PushBack(trackerKey{
			reqId: v.ReqId,
		})
		tracker.listElement = newElem
	}
	tracker.logs = append(tracker.logs, v)
	agg.logsByRequest[v.ReqId] = tracker

	// Update the record of the last time we saw a log for this request
	trackerElem := tracker.listElement.Value.(trackerKey)
	trackerElem.lastLogTime = time.Now()
	tracker.listElement.Value = trackerElem
}

// A data aggregator that outputs basic statistics such as the average
// Intended to be used by data extractors.
type basicStats struct {
	sum   uint64
	count uint64
}

func (bs *basicStats) NewPoint(data uint64) {
	bs.sum += data
	bs.count++
}

func (bs *basicStats) Average() uint64 {
	if bs.count == 0 {
		return 0
	}

	return bs.sum / bs.count
}

func (bs *basicStats) Count() uint64 {
	return bs.count
}
