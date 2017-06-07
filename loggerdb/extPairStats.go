// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPairStats is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qloggerdb

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs/qlog"
)

const statPeriodSec = 5

type extPairStats struct {
	db        DbInterface
	fmtStart  string
	fmtStop   string
	sameScope bool

	stats    basicStats
	statFrom time.Time
}

// Set matchingIndent to true if start and stop should only be recognized when they
// are seen at the same function scope
func NewExtPairStats(db_ DbInterface, start string, stop string,
	matchingIndent bool) *extPairStats {

	return &extPairStats{
		db:        db_,
		fmtStart:  start,
		fmtStop:   stop,
		sameScope: matchingIndent,
		statFrom:  time.Now(),
	}
}

func (ext *extPairStats) ExtractStatFrom(request qlog.LogStack, idx int) {
	indentCount := 0
	for i := idx; i < len(request); i++ {
		if request[i].Format == ext.fmtStop && ((!ext.sameScope) ||
			(ext.sameScope && indentCount == 1)) {

			// found a match
			delta := request[i].T - request[idx].T
			if delta < 0 {
				fmt.Printf("Negative delta (%d): |%s| to |%s|\n",
					delta, ext.fmtStart, ext.fmtStop)
			} else {
				ext.stats.NewPoint(uint64(delta))
			}
			return
		}

		if qlog.IsFunctionIn(request[i].Format) {
			indentCount++
		} else if qlog.IsFunctionOut(request[i].Format) {
			indentCount--
		}
	}

	fmt.Printf("Broken function pair: '%s' without '%s'\n", ext.fmtStart,
		ext.fmtStop)
}

func (ext *extPairStats) ProcessRequest(request qlog.LogStack) {
	// Check if the stats need to be finalized and reset
	if time.Since(ext.statFrom).Seconds() > statPeriodSec {
		if ext.stats.Count() > 0 {
			tags := make([]tag, 0)
			tags = append(tags, newTag("fmtStart", ext.fmtStart))
			tags = append(tags, newTag("fmtStop", ext.fmtStop))

			fields := make([]field, 0)
			fields = append(fields, newField("average",
				ext.stats.Average()))
			fields = append(fields, newField("samples",
				ext.stats.Count()))

			ext.db.Store(tags, fields)
		}

		ext.stats = basicStats{}
		ext.statFrom = time.Now()
	}

	for i, v := range request {
		if v.Format == ext.fmtStart {
			ext.ExtractStatFrom(request, i)
		}
	}
}
