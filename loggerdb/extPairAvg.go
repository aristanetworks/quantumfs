// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPairAvg is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qloggerdb

import (
	"fmt"

	"github.com/aristanetworks/quantumfs/qlog"
)

type extPairAvg struct {
	db       DbInterface
	fmtStart string
	fmtStop  string
}

func NewExtPairAvg(db DbInterface, start string, stop string) *extPairAvg {
	return &extPairAvg{
		fmtStart: start,
		fmtStop:  stop,
	}
}

func (ext *extPairAvg) ProcessRequest(request qlog.LogStack) {
	for _, v := range request {
		fmt.Printf("%s", v.ToString())
	}

	// TODO: Extract the stat and output to memdb
	// ext.db.Store(ext.fmtStart, newStatistic)
}
