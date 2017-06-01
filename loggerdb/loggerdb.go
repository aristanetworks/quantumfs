// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qloggerdb

import "github.com/aristanetworks/quantumfs/qlog"

const requestEndAfterNs = 1000000000

type DbInterface interface {
	Store(tag string, timeNs uint64)

	Fetch(tag string, lastN int) []uint64
}

type LoggerDb struct {
	logsByRequest	map[uint64]qlog.LogStack
}

func NewLoggerDb(db DbInterface) *LoggerDb {
	return &LoggerDb {
		logsByRequest:	make(map[uint64]qlog.LogStack, 0),
	}
}

func (logger *LoggerDb) ProcessLog(v qlog.LogOutput) {
	var logs qlog.LogStack
	if series, exists := logger.logsByRequest[v.ReqId]; exists {
		logs = series
	} else {
		logs = make([]qlog.LogOutput, 0)
	}

	logs = append(logs, v)
}
