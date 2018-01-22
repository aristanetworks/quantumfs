// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"container/list"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type TriggerType int

const (
	OnFormat        = TriggerType(iota) // Match full log format
	OnPartialFormat                     // Match log format substring
	OnAll                               // Match every message
)

type CommandType int

const (
	MessageCommandType = CommandType(iota)
	PublishCommandType
	GcCommandType
)

type StatCommand interface {
	Type() CommandType
	Data() interface{}
}

type MessageCommand struct {
	log *qlog.LogOutput
}

type GcCommand struct{}

func (cmd *MessageCommand) Type() CommandType {
	return MessageCommandType
}

func (cmd *MessageCommand) Data() interface{} {
	return cmd.log
}

type PublishResult struct {
	measurement string
	tags        []quantumfs.Tag
	fields      []quantumfs.Field
}

type PublishCommand struct {
	result chan PublishResult
}

func (cmd *PublishCommand) Type() CommandType {
	return PublishCommandType
}

func (cmd *PublishCommand) Data() interface{} {
	return cmd.result
}

func (cmd *GcCommand) Type() CommandType {
	return GcCommandType
}

func (cmd *GcCommand) Data() interface{} {
	return nil
}

type StatExtractor interface {
	// This is the list of strings that the extractor will be triggered on and
	// receive. Note that full formats include a trailing \n.
	TriggerStrings() []string
	Chan() chan StatCommand
	Type() TriggerType
}

func AggregateLogs(mode qlog.LogProcessMode, filename string,
	db quantumfs.TimeSeriesDB, extractors []StatExtractor,
	publishInterval time.Duration) *Aggregator {

	reader := qlog.NewReader(filename)
	agg := NewAggregator(db, extractors, reader.DaemonVersion(), publishInterval)

	reader.ProcessLogs(mode, func(log *qlog.LogOutput) {
		if log == nil {
			panic("nil log")
		}
		agg.ProcessLog(log)
	})

	return agg
}

type Aggregator struct {
	db            quantumfs.TimeSeriesDB
	daemonVersion string

	// track the oldest untouched requests so we can push them to the stat
	// extractors after the resting period (so we're confident there are no
	// more logs coming for each request)
	requestSequence list.List

	extractors             []StatExtractor
	triggerByFormat        map[string][]chan StatCommand
	triggerByPartialFormat map[string][]chan StatCommand
	triggerAll             []chan StatCommand

	gcInternval     time.Duration
	publishInterval time.Duration

	queueMutex   utils.DeferableMutex
	queueService time.Time
	queueLogs    []*qlog.LogOutput
	notification chan struct{}
}

func NewAggregator(db_ quantumfs.TimeSeriesDB,
	extractors []StatExtractor, daemonVersion_ string,
	publishInterval time.Duration) *Aggregator {

	agg := Aggregator{
		db:                     db_,
		daemonVersion:          daemonVersion_,
		extractors:             extractors,
		triggerByFormat:        make(map[string][]chan StatCommand),
		triggerByPartialFormat: make(map[string][]chan StatCommand),
		triggerAll:             make([]chan StatCommand, 0),
		gcInternval:            time.Minute * 2,
		publishInterval:        publishInterval,
		queueLogs:              make([]*qlog.LogOutput, 0, 1000),
		queueService:           time.Now(),
		notification:           make(chan struct{}, 1),
	}

	// Record the desired filtering
	for _, extractor := range agg.extractors {
		c := extractor.Chan()

		if extractor.Type() == OnAll {
			agg.triggerAll = append(agg.triggerAll, c)
			continue
		}

		triggers := extractor.TriggerStrings()
		for _, trigger := range triggers {
			var triggerList map[string][]chan StatCommand
			if extractor.Type() == OnFormat {
				triggerList = agg.triggerByFormat
			} else { // OnPartialFormat
				triggerList = agg.triggerByPartialFormat
			}

			newTriggers, exists := triggerList[trigger]
			if !exists {
				newTriggers = make([]chan StatCommand, 0)
			}

			newTriggers = append(newTriggers, c)

			if extractor.Type() == OnFormat {
				agg.triggerByFormat[trigger] = newTriggers
			} else { // OnPartialFormat
				agg.triggerByPartialFormat[trigger] = newTriggers
			}
		}
	}

	go agg.processThread()
	go agg.publish()
	go agg.runGC()

	return &agg
}

func (agg *Aggregator) ProcessLog(log *qlog.LogOutput) {
	defer agg.queueMutex.Lock().Unlock()

	agg.queueLogs = append(agg.queueLogs, log)

	if time.Since(agg.queueService) > time.Minute {
		panic("Qloggerdb queue not serviced for over a minute!")
	}

	select {
	case agg.notification <- struct{}{}:
	default:
	}
}

func (agg *Aggregator) processThread() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("processThread panicked: ", r)
		}
	}()

	for {
		logs := func() []*qlog.LogOutput {
			<-agg.notification

			defer agg.queueMutex.Lock().Unlock()

			// nothing to do
			if len(agg.queueLogs) == 0 {
				return []*qlog.LogOutput{}
			}

			// Take a small performance hit in creating a new array,
			// but gain a much quicker mutex unlock
			rtn := agg.queueLogs
			agg.queueLogs = make([]*qlog.LogOutput, 0, 1000)
			agg.queueService = time.Now()
			return rtn
		}()

		for _, log := range logs {
			agg.filterAndDistribute(log)
		}
	}
}

func (agg *Aggregator) filterAndDistribute(log *qlog.LogOutput) {
	// These always match
	for _, extractor := range agg.triggerAll {
		extractor <- &MessageCommand{
			log: log,
		}
	}

	// These match the format string fully
	matching := agg.triggerByFormat[log.Format]
	for _, extractor := range matching {
		extractor <- &MessageCommand{
			log: log,
		}
	}

	// These partially match the format string
	for trigger, extractors := range agg.triggerByPartialFormat {
		if strings.Contains(log.Format, trigger) {
			for _, extractor := range extractors {
				extractor <- &MessageCommand{
					log: log,
				}
			}
		}
	}
}

func (agg *Aggregator) publish() {
	for {
		time.Sleep(agg.publishInterval)
		nowTime := time.Now()

		results := make([]chan PublishResult, 0, len(agg.extractors))
		// Trigger extractors to publish in parallel
		for _, extractor := range agg.extractors {
			targetChan := extractor.Chan()
			resultChannel := make(chan PublishResult)
			targetChan <- &PublishCommand{
				result: resultChannel,
			}

			results = append(results, resultChannel)
		}

		// Wait for all their results to come in
		for _, resultChannel := range results {
			result := <-resultChannel
			if result.tags != nil && len(result.tags) > 0 {
				// add the qfs version tag
				result.tags = append(result.tags,
					quantumfs.NewTag("version",
						agg.daemonVersion))

				agg.db.Store(result.measurement, result.tags,
					result.fields, nowTime)
			}
		}
	}
}

func (agg *Aggregator) runGC() {
	for {
		time.Sleep(agg.gcInternval)

		for _, extractor := range agg.extractors {
			targetChan := extractor.Chan()
			targetChan <- &GcCommand{}
		}
	}
}

type byIncreasing []int64

func (a byIncreasing) Len() int           { return len(a) }
func (a byIncreasing) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byIncreasing) Less(i, j int) bool { return a[i] < a[j] }

// A data aggregator that outputs histogram based statistics
type histoStats struct {
	minVal	int64
	maxVal	int64
	bucketWidth	int64
	buckets	[]int64

	// Keep a bucket for data outside the range
	beforeCount	int64
	pastCount	int64

	count	int64
}

func NewHistoStats(min int64, max int64, buckets_ int64) histoStats {
	numRange := (1 + max) - min
	width := numRange / buckets_
	// when the range doesn't divide evenly, choose to have a smaller upper
	// bucket than a really big one.
	if numRange % buckets_ != 0 {
		width++
	}

	return histoStats {
		minVal:	min,
		maxVal:	max,
		bucketWidth:	width,
		buckets:	make([]int64, buckets_),
	}
}

func (hs *histoStats) NewPoint(data int64) {
	if data < hs.minVal {
		hs.beforeCount++
	} else if data > hs.maxVal {
		hs.pastCount++
	} else {
		idx := (data - hs.minVal) / hs.bucketWidth
		hs.buckets[idx]++
	}

	hs.count++
}

func (hs *histoStats) Count() int64 {
	return hs.count
}

func (hs *histoStats) Clear() {
	hs.buckets = make([]int64, len(hs.buckets))
	hs.beforeCount = 0
	hs.pastCount = 0
	hs.count = 0
}

func (hs *histoStats) Histogram() map[string]int64 {
	rtn := make(map[string]int64)

	min := hs.minVal
	for _, count := range hs.buckets {
		nextMin := min + hs.bucketWidth

		tag := strconv.Itoa(int(min)) + "-" + strconv.Itoa(int(nextMin))
		if hs.count == 0 {
			rtn[tag] = 0
		} else {
			// Normalize the histogram to a percentage for
			// easier interpretation
			rtn[tag] = (100 * count) / hs.count
		}
		min = nextMin
	}

	rtn["BeforeHistogram"] = hs.beforeCount
	rtn["PastHistogram"] = hs.pastCount

	return rtn
}

// A data aggregator that outputs basic statistics such as the average
// Intended to be used by data extractors.
type basicStats struct {
	sum    int64
	points []int64
	max    int64
}

func (bs *basicStats) NewPoint(data int64) {
	bs.sum += data
	bs.points = append(bs.points, data)

	if data > bs.max {
		bs.max = data
	}
}

func (bs *basicStats) Max() int64 {
	return bs.max
}

func (bs *basicStats) Average() int64 {
	if len(bs.points) == 0 {
		return 0
	}

	return bs.sum / int64(len(bs.points))
}

func (bs *basicStats) Count() int64 {
	return int64(len(bs.points))
}

func (bs *basicStats) Percentiles() map[string]int64 {
	rtn := make(map[string]int64)
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
