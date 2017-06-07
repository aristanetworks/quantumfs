// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qloggerdb

import (
	"container/list"

	"github.com/aristanetworks/quantumfs/qlog"
)

const requestEndAfterNs = 3000000000

type Field struct {
	Name string
	Data uint64
}

func newField(name_ string, data_ uint64) Field {
	return Field{
		Name: name_,
		Data: data_,
	}
}

type Tag struct {
	Name string
	Data string
}

func newTag(name_ string, data_ string) Tag {
	return Tag{
		Name: name_,
		Data: data_,
	}
}

type DbInterface interface {
	Store(tags []Tag, fields []Field)

	Fetch(withTags []Tag, field string, lastN int) []uint64
}

type logTrack struct {
	logs        qlog.LogStack
	listElement *list.Element
}

type trackerKey struct {
	reqId       uint64
	lastLogTime int64
}

type StatExtractor interface {
	ProcessRequest(request qlog.LogStack)
}

type LoggerDb struct {
	logsByRequest map[uint64]logTrack

	// track the oldest untouched requests so we can push them to the stat
	// extractors after the resting period (so we're confident there are no
	// more logs coming for each request)
	requestSequence list.List

	statExtractors []StatExtractor
}

func NewLoggerDb(db DbInterface, extractors []StatExtractor) *LoggerDb {
	rtn := LoggerDb{
		logsByRequest:  make(map[uint64]logTrack),
		statExtractors: extractors,
	}

	return &rtn
}

func (logger *LoggerDb) ProcessLog(v qlog.LogOutput) {
	var tracker logTrack
	if tracker_, exists := logger.logsByRequest[v.ReqId]; exists {
		tracker = tracker_
		logger.requestSequence.MoveToBack(tracker.listElement)
	} else {
		tracker.logs = make([]qlog.LogOutput, 0)
		newElem := logger.requestSequence.PushBack(trackerKey{
			reqId:       v.ReqId,
			lastLogTime: v.T,
		})
		tracker.listElement = newElem
	}
	tracker.logs = append(tracker.logs, v)
	logger.logsByRequest[v.ReqId] = tracker

	// Update the record of the last time we saw a log for this request
	trackerElem := tracker.listElement.Value.(trackerKey)
	if v.T > trackerElem.lastLogTime {
		trackerElem.lastLogTime = v.T
	}

	// now check if any requests are old and ready to go to the extractors
	for {
		requestElem := logger.requestSequence.Front()

		if requestElem == nil {
			break
		}

		request := requestElem.Value.(trackerKey)
		if v.T-request.lastLogTime > requestEndAfterNs {
			logger.requestSequence.Remove(requestElem)
			reqLogs := logger.logsByRequest[request.reqId]
			delete(logger.logsByRequest, request.reqId)

			for _, e := range logger.statExtractors {
				e.ProcessRequest(reqLogs.logs)
			}
		} else {
			// No more requests ready to extract stats
			break
		}
	}
}
