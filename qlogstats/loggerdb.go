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
	"unsafe"

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

type Measurement struct {
	name   string
	tags   []quantumfs.Tag
	fields []quantumfs.Field
}

func appendNewTag(tags []quantumfs.Tag, name string, data string) []quantumfs.Tag {
	return append(tags, quantumfs.NewTag(name, data))
}

func appendNewFieldInt(fields []quantumfs.Field, name string,
	data int64) []quantumfs.Field {

	return append(fields, quantumfs.NewFieldInt(name, data))
}

func appendNewFieldString(fields []quantumfs.Field, name string,
	data string) []quantumfs.Field {

	return append(fields, quantumfs.NewFieldString(name, data))
}

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

type PublishCommand struct {
	result chan []Measurement
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
	// Call this after the StatExtractor is fully initialized
	run()

	// Do the real processing
	process(msg *qlog.LogOutput)
	publish() []Measurement
	gc() // Optional

	// ExtractorBase below implements these

	// This is the list of strings that the extractor will be triggered on and
	// receive. Note that full formats include a trailing \n.
	TriggerStrings() []string
	Type() TriggerType
	Chan() chan StatCommand

	// Internal
	_gc()
}

// A base class which handles the boiler plate for writing StatExtractors
type StatExtractorBase struct {
	Name     string
	messages chan StatCommand
	self     StatExtractor // Our superclass
	type_    TriggerType
	triggers []string
}

func NewStatExtractorBase(name string, self StatExtractor, type_ TriggerType,
	triggerStrings []string) StatExtractorBase {

	return StatExtractorBase{
		Name:     name,
		messages: make(chan StatCommand, 10000),
		self:     self,
		type_:    type_,
		triggers: triggerStrings,
	}
}

func (seb *StatExtractorBase) process(msg *qlog.LogOutput) {}

func (seb *StatExtractorBase) publish() []Measurement {
	return []Measurement{}
}

func (seb *StatExtractorBase) gc() {}

func (seb *StatExtractorBase) _gc() {}

func (seb *StatExtractorBase) run() {
	go seb.listen()
}

func (seb *StatExtractorBase) listen() {
	for {
		cmd := <-seb.messages
		switch cmd.Type() {
		case MessageCommandType:
			seb.self.process(cmd.Data().(*qlog.LogOutput))
		case PublishCommandType:
			resultChannel := cmd.Data().(chan []Measurement)
			resultChannel <- seb.self.publish()
		case GcCommandType:
			seb.self._gc()
		}
	}
}

func (seb *StatExtractorBase) Chan() chan StatCommand {
	return seb.messages
}

func (seb *StatExtractorBase) TriggerStrings() []string {
	return seb.triggers
}

func (seb *StatExtractorBase) Type() TriggerType {
	return seb.type_
}

type StatExtractorBaseWithGC struct {
	StatExtractorBase
	CurrentGeneration uint64
}

func NewStatExtractorBaseWithGC(name string, self StatExtractor, type_ TriggerType,
	triggerStrings []string) StatExtractorBaseWithGC {

	sebgc := StatExtractorBaseWithGC{
		StatExtractorBase: NewStatExtractorBase(name, self,
			type_, triggerStrings),
	}

	return sebgc
}

// Returns true if the given generation has reached death when
// compared with StatExtractorBaseWithGC.CurrentGeneration.
func (sebgc *StatExtractorBaseWithGC) AgedOut(generation uint64) bool {
	if generation+2 < sebgc.CurrentGeneration {
		return true
	}
	return false
}

func (sebgc *StatExtractorBaseWithGC) _gc() {
	sebgc.CurrentGeneration++
	sebgc.self.gc()
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
	queueLogs    []*qlog.LogOutput
	queueSize    int64
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

const maxQueueSize_bytes = 10 * 1024 * 1024 * 1024

func logSize(log *qlog.LogOutput) int64 {
	rtn := int(unsafe.Sizeof(log))
	rtn += len(log.Format)
	for _, arg := range log.Args {
		rtn += int(unsafe.Sizeof(arg))
	}

	return int64(rtn)
}

func (agg *Aggregator) ProcessLog(log *qlog.LogOutput) {
	defer agg.queueMutex.Lock().Unlock()

	agg.queueLogs = append(agg.queueLogs, log)

	agg.queueSize += logSize(log)
	if agg.queueSize > maxQueueSize_bytes {
		panic("Qlogger processThread probably locked due to timeout")
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
			agg.queueSize = 0

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
	versionTag := quantumfs.NewTag("version", agg.daemonVersion)

	for {
		time.Sleep(agg.publishInterval)
		nowTime := time.Now()

		results := make([]chan []Measurement, 0, len(agg.extractors))
		// Trigger extractors to publish in parallel
		for _, extractor := range agg.extractors {
			targetChan := extractor.Chan()
			resultChannel := make(chan []Measurement, 1)
			targetChan <- &PublishCommand{
				result: resultChannel,
			}

			results = append(results, resultChannel)
		}

		// Wait for all their results to come in
		for _, resultChannel := range results {
			measurements := <-resultChannel

			for _, measurement := range measurements {
				name := measurement.name
				tags := measurement.tags
				fields := measurement.fields

				if tags == nil || len(tags) == 0 {
					continue
				}

				// add the qfs version tag
				tags = append(tags, versionTag)

				agg.db.Store(name, tags, fields, nowTime)
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
type histogram struct {
	minVal      int64
	maxVal      int64
	bucketWidth int64
	buckets     []int64

	// Keep a bucket for data outside the range
	beforeCount int64
	pastCount   int64

	count     int64
	normalize bool
}

func NewHistogram(min int64, max int64, buckets_ int64, normalize bool) histogram {
	numRange := (1 + max) - min
	width := numRange / buckets_
	// when the range doesn't divide evenly, choose to have a smaller upper
	// bucket than a really big one.
	if numRange%buckets_ != 0 {
		width++
	}

	return histogram{
		minVal:      min,
		maxVal:      max,
		bucketWidth: width,
		buckets:     make([]int64, buckets_),
		normalize:   normalize,
	}
}

func (hs *histogram) NewPoint(data int64) {
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

func (hs *histogram) Count() int64 {
	return hs.count
}

func (hs *histogram) Clear() {
	hs.buckets = make([]int64, len(hs.buckets))
	hs.beforeCount = 0
	hs.pastCount = 0
	hs.count = 0
}

func (hs *histogram) Histogram() map[string]int64 {
	rtn := make(map[string]int64)

	min := hs.minVal
	for _, count := range hs.buckets {
		nextMin := min + hs.bucketWidth

		tag := strconv.Itoa(int(min)) + "-" + strconv.Itoa(int(nextMin))
		if hs.count == 0 {
			rtn[tag] = 0
		} else if hs.normalize {
			// Normalize the histogram to a percentage for
			// easier interpretation
			rtn[tag] = (100 * count) / hs.count
		} else {
			rtn[tag] = count
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
