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

const requestEndAfterNs = 3000000000

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

type LoggerDb struct {
	db            quantumfs.TimeSeriesDB
	logsByRequest map[uint64]logTrack

	// track the oldest untouched requests so we can push them to the stat
	// extractors after the resting period (so we're confident there are no
	// more logs coming for each request)
	requestSequence list.List

	statExtractors []StatExtractorConfig

	queueMutex utils.DeferableMutex
	queueLogs  []qlog.LogOutput
}

func NewLoggerDb(db_ quantumfs.TimeSeriesDB,
	extractors []StatExtractorConfig) *LoggerDb {

	rtn := LoggerDb{
		db:             db_,
		logsByRequest:  make(map[uint64]logTrack),
		statExtractors: extractors,
		queueLogs:      make([]qlog.LogOutput, 0),
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

func (logger *LoggerDb) ProcessThread() {
	for {
		logs := func() []qlog.LogOutput {
			defer logger.queueMutex.Lock().Unlock()

			// nothing to do
			if len(logger.queueLogs) == 0 {
				return []qlog.LogOutput{}
			}

			// Take a small performance hit in creating a new array,
			// but gain a much quicker mutex unlock
			rtn := logger.queueLogs
			logger.queueLogs = make([]qlog.LogOutput, 0)
			return rtn
		}()

		for _, log := range logs {
			logger.processLog(log)
		}

		// Now check if any requests are old and ready to go to extractors
		now := time.Now()
		for {
			requestElem := logger.requestSequence.Front()

			if requestElem == nil {
				break
			}

			request := requestElem.Value.(trackerKey)
			if now.Sub(request.lastLogTime) > time.Duration(0+
				requestEndAfterNs) {

				logger.requestSequence.Remove(requestElem)
				reqLogs := logger.logsByRequest[request.reqId]
				delete(logger.logsByRequest, request.reqId)

				for _, e := range logger.statExtractors {
					e.extractor.ProcessRequest(reqLogs.logs)
				}
			} else {
				// No more requests ready to extract stats
				break
			}
		}

		// Lastly check if any extractors need to Publish
		for i, extractor := range logger.statExtractors {
			if now.Sub(extractor.lastOutput) > extractor.statPeriod {
				tags, fields := extractor.extractor.Publish()
				if tags != nil && len(tags) > 0 {
					logger.db.Store(tags, fields)
				}

				extractor.lastOutput = now
				logger.statExtractors[i] = extractor
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (logger *LoggerDb) ProcessLog(v qlog.LogOutput) {
	defer logger.queueMutex.Lock().Unlock()

	logger.queueLogs = append(logger.queueLogs, v)
}

func (logger *LoggerDb) processLog(v qlog.LogOutput) {
	var tracker logTrack
	if tracker_, exists := logger.logsByRequest[v.ReqId]; exists {
		tracker = tracker_
		logger.requestSequence.MoveToBack(tracker.listElement)
	} else {
		tracker.logs = make([]qlog.LogOutput, 0)
		newElem := logger.requestSequence.PushBack(trackerKey{
			reqId: v.ReqId,
		})
		tracker.listElement = newElem
	}
	tracker.logs = append(tracker.logs, v)
	logger.logsByRequest[v.ReqId] = tracker

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
