// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"container/list"
	"sort"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type indentedLog struct {
	log    qlog.LogOutput
	indent int
}

type logTrack struct {
	logs        qlog.LogStack
	listElement *list.Element
}

type trackerKey struct {
	reqId       uint64
	lastLogTime time.Time
}

type StatExtractor interface {
	// This is the list of strings that the extractor will be triggered on and
	// receive. Strings must match format exactly, with a trailing "\n"
	TriggerStrings() []string

	ProcessRequest(request []indentedLog)

	Publish() (string, []quantumfs.Tag, []quantumfs.Field)
}

type StatExtractorConfig struct {
	extractor  StatExtractor
	statPeriod time.Duration
	lastOutput time.Time
}

func NewStatExtractorConfig(ext StatExtractor,
	period time.Duration) *StatExtractorConfig {

	return &StatExtractorConfig{
		extractor:  ext,
		statPeriod: period,
	}
}

func AggregateLogs(mode qlog.LogProcessMode, filename string,
	db quantumfs.TimeSeriesDB, extractors []StatExtractorConfig) *Aggregator {

	reader := qlog.NewReader(filename)
	agg := NewAggregator(db, extractors, reader.DaemonVersion())

	reader.ProcessLogs(mode, func(v qlog.LogOutput) {
		agg.ProcessLog(v)
	})

	return agg
}

type extractorIdx int

type Aggregator struct {
	db            quantumfs.TimeSeriesDB
	logsByRequest map[uint64]logTrack
	daemonVersion string

	// track the oldest untouched requests so we can push them to the stat
	// extractors after the resting period (so we're confident there are no
	// more logs coming for each request)
	requestSequence list.List

	// Uses a prefix (slower) system to extract data
	errorCount *extPointStats

	statExtractors  []StatExtractorConfig
	statTriggers    map[string][]extractorIdx
	requestEndAfter time.Duration

	queueMutex utils.DeferableMutex
	queueLogs  []qlog.LogOutput
}

const errorStr = "ERROR: "

func NewAggregator(db_ quantumfs.TimeSeriesDB,
	extractors []StatExtractorConfig, daemonVersion_ string) *Aggregator {

	rtn := Aggregator{
		db:              db_,
		logsByRequest:   make(map[uint64]logTrack),
		daemonVersion:   daemonVersion_,
		errorCount:      NewExtPointStats(errorStr, "SystemErrors"),
		statExtractors:  extractors,
		statTriggers:    make(map[string][]extractorIdx),
		requestEndAfter: time.Second * 30,
		queueLogs:       make([]qlog.LogOutput, 0),
	}

	// Sync all extractors and setup their triggers
	now := time.Now()
	for i, v := range rtn.statExtractors {
		v.lastOutput = now
		rtn.statExtractors[i] = v

		triggers := v.extractor.TriggerStrings()
		for _, trigger := range triggers {
			newTriggers, exists := rtn.statTriggers[trigger]
			if !exists {
				newTriggers = make([]extractorIdx, 0)
			}

			newTriggers = append(newTriggers, extractorIdx(i))
			rtn.statTriggers[trigger] = newTriggers
		}
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
				agg.requestEndAfter) {

				agg.requestSequence.Remove(requestElem)
				reqLogs := agg.logsByRequest[request.reqId]
				delete(agg.logsByRequest, request.reqId)

				agg.FilterRequest(reqLogs.logs)
			} else {
				// No more requests ready to extract stats
				break
			}
		}

		// Lastly check if any extractors need to Publish
		for i, extractor := range agg.statExtractors {
			if now.Sub(extractor.lastOutput) > extractor.statPeriod {
				measurement, tags,
					fields := extractor.extractor.Publish()
				if tags != nil && len(tags) > 0 {
					// add the qfs version tag
					tags = append(tags,
						quantumfs.NewTag("version",
							agg.daemonVersion))

					agg.db.Store(measurement, tags, fields)
				}

				extractor.lastOutput = now
				agg.statExtractors[i] = extractor
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// Use the trigger map to determine if any parts of this request need to go out
func (agg *Aggregator) FilterRequest(logs []qlog.LogOutput) {
	// This is a map that contains, for each extractor that cares about a given
	// log in logs, only the filtered logs that that extractor has said it wants
	filteredRequests := make(map[extractorIdx][]indentedLog)

	indentCount := 0
	for _, curlog := range logs {
		// Find if any extractors have registered for this log.

		if qlog.IsFunctionOut(curlog.Format) {
			indentCount--
		}

		// Check for partial matching for errors
		if curlog.Format[:len(errorStr)] == errorStr {
			agg.errorCount.ProcessRequest([]indentedLog{
				indentedLog{
					log:    curlog,
					indent: indentCount,
				},
			})
		}

		// The statTriggers map is a map that uses a log format string for
		// the key. The value is a list of extractors who want the log
		if extractors, exists := agg.statTriggers[curlog.Format]; exists {
			for _, triggered := range extractors {
				// This extractor wants this log, so append it
				filtered, hasLogs := filteredRequests[triggered]
				if !hasLogs {
					filtered = make([]indentedLog, 0)
				}

				filtered = append(filtered, indentedLog{
					log:    curlog,
					indent: indentCount,
				})
				filteredRequests[triggered] = filtered
			}
		}

		// Keep track of what level we're indented
		if qlog.IsFunctionIn(curlog.Format) {
			indentCount++
		}
	}

	// Send the filtered logs out
	for extractor, toSend := range filteredRequests {
		agg.statExtractors[extractor].extractor.ProcessRequest(toSend)
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

type byIncreasing []uint64

func (a byIncreasing) Len() int           { return len(a) }
func (a byIncreasing) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byIncreasing) Less(i, j int) bool { return a[i] < a[j] }

// A data aggregator that outputs basic statistics such as the average
// Intended to be used by data extractors.
type basicStats struct {
	sum    uint64
	points []uint64
	max    uint64
}

func (bs *basicStats) NewPoint(data uint64) {
	bs.sum += data
	bs.points = append(bs.points, data)

	if data > bs.max {
		bs.max = data
	}
}

func (bs *basicStats) Max() uint64 {
	return bs.max
}

func (bs *basicStats) Average() uint64 {
	if len(bs.points) == 0 {
		return 0
	}

	return bs.sum / uint64(len(bs.points))
}

func (bs *basicStats) Count() uint64 {
	return uint64(len(bs.points))
}

func (bs *basicStats) Percentiles() map[string]uint64 {
	rtn := make(map[string]uint64)
	points := bs.points

	if len(points) == 0 {
		points = append(points, 0)
	}

	// sort the points
	sort.Sort(byIncreasing(points))

	lastIdx := float32(len(points) - 1)

	rtn["50pct_ns"] = points[int(lastIdx*0.50)]
	rtn["90pct_ns"] = points[int(lastIdx*0.90)]
	rtn["95pct_ns"] = points[int(lastIdx*0.95)]
	rtn["99pct_ns"] = points[int(lastIdx*0.99)]

	return rtn
}
