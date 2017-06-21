// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extPairStats is a stat extractor that extracts the average duration between two
// log format strings. This would most often be used to time between a fnIn and fnOut

package qlogstats

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
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

func (ext *extPairStats) ExtractStatFrom(request []indentedLog, idx int) {
	for i := idx; i < len(request); i++ {
		if request[i].log.Format == ext.fmtStop &&
			(!ext.sameScope ||
				request[idx].indent == request[i].indent) {

			// found a match
			delta := request[i].log.T - request[idx].log.T
			if delta < 0 {
				fmt.Printf("Negative delta (%d): |%s| to |%s|\n",
					delta, ext.fmtStart, ext.fmtStop)
			} else {
				ext.stats.NewPoint(uint64(delta))
			}
			return
		}
	}

	fmt.Printf("Broken function pair: '%s' without '%s'\n", ext.fmtStart,
		ext.fmtStop)
}

func (ext *extPairStats) TriggerStrings() []string {
	rtn := make([]string, 0)

	rtn = append(rtn, ext.fmtStart)
	rtn = append(rtn, ext.fmtStop)
	return rtn
}

func (ext *extPairStats) ProcessRequest(request []indentedLog) {
	for i, v := range request {
		if v.log.Format == ext.fmtStart {
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

	for name, data := range ext.stats.Percentiles() {
		fields = append(fields, quantumfs.NewField(name, data))
	}

	ext.stats = basicStats{}
	return tags, fields
}
