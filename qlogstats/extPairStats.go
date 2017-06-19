// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPairStats is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qlogstats

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type extPairStats struct {
	fmtStart  string
	fmtStop   string
	sameScope bool
	name      string

	stats basicStats
}

// Set matchingIndent to true if start and stop should only be recognized when they
// are seen at the same function scope
func NewExtPairStats(start string, stop string, matchingIndent bool,
	nametag string) *extPairStats {

	return &extPairStats{
		fmtStart:  start,
		fmtStop:   stop,
		sameScope: matchingIndent,
		name:      nametag,
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
	for i, v := range request {
		if v.Format == ext.fmtStart {
			ext.ExtractStatFrom(request, i)
		}
	}
}

func (ext *extPairStats) Publish() (tags []quantumfs.Tag, fields []quantumfs.Field) {
	tags = make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("name", ext.name))

	fields = make([]quantumfs.Field, 0)

	fields = append(fields, quantumfs.NewField("average", ext.stats.Average()))
	fields = append(fields, quantumfs.NewField("samples", ext.stats.Count()))

	ext.stats = basicStats{}
	return tags, fields
}
