// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// extWorkspaceStats is a stat extractor that extracts the count and percentiles
// latencies of FUSE requests on a per-workspace basis.

package qlogstats

import (
	"fmt"
	"strings"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type newRequest struct {
	time                 int64
	requestType          string
	lastUpdateGeneration uint64
}

type outstandingRequest struct {
	start                int64
	workspace            string
	requestType          string // ie Mux::Read
	lastUpdateGeneration uint64
}

type extWorkspaceStats struct {
	StatExtractorBase

	lock                utils.DeferableMutex
	newRequests         map[uint64]newRequest
	outstandingRequests map[uint64]outstandingRequest

	stats map[string]map[string]*basicStats // ie [workspace]["Mux::Read"]
}

func NewExtWorkspaceStats(nametag string) StatExtractor {
	ext := &extWorkspaceStats{
		newRequests:         make(map[uint64]newRequest),
		outstandingRequests: make(map[uint64]outstandingRequest),
		stats:               make(map[string]map[string]*basicStats),
	}

	strings := make([]string, 2)
	strings = append(strings, "Mux::")
	strings = append(strings, daemon.FuseRequestWorkspace)

	ext.StatExtractorBase = NewStatExtractorBase(nametag, ext, OnPartialFormat,
		strings)

	ext.run()

	return ext
}

func (ext *extWorkspaceStats) process(msg *qlog.LogOutput) {
	if msg.ReqId >= qlog.MinSpecialReqId {
		// The utility request ranges are never FUSE requests
		return
	}

	switch {
	default:
		fmt.Printf("Unexpected log message in extWorkspaceStats %d '%s'\n",
			msg.ReqId, msg.Format)

	case strings.HasPrefix(msg.Format, qlog.FnEnterStr):
		// Start of a FUSE request, we don't know the workspace yet
		_, preexists := ext.newRequests[msg.ReqId]
		if preexists {
			fmt.Printf("Already have a newRequest for id %d\n",
				msg.ReqId)
			return
		}

		request := strings.SplitN(msg.Format, " ", 3)[1]

		ext.newRequests[msg.ReqId] = newRequest{
			time:                 msg.T,
			requestType:          request,
			lastUpdateGeneration: ext.CurrentGeneration,
		}

	case strings.Compare(msg.Format, daemon.FuseRequestWorkspace+"\n") == 0:
		// This message contains the request ID -> workspace mapping
		startMsg, exists := ext.newRequests[msg.ReqId]
		if !exists {
			fmt.Printf("Got name mapping without request start %d %s\n",
				msg.ReqId, msg.Args[0])
			return
		}

		ext.outstandingRequests[msg.ReqId] = outstandingRequest{
			start:                startMsg.time,
			workspace:            msg.Args[0].(string),
			requestType:          startMsg.requestType,
			lastUpdateGeneration: ext.CurrentGeneration,
		}
		delete(ext.newRequests, msg.ReqId)

	case strings.HasPrefix(msg.Format, qlog.FnExitStr):
		// End of a FUSE request
		request, exists := ext.outstandingRequests[msg.ReqId]
		if !exists {
			fmt.Printf("Closing request which isn't outstanding %d %s\n",
				msg.ReqId, msg.Format)
			return
		}

		delta := msg.T - request.start

		workspaceStats, exists := ext.stats[request.workspace]
		if !exists {
			workspaceStats = make(map[string]*basicStats)
			ext.stats[request.workspace] = workspaceStats
		}

		stat := workspaceStats[request.requestType]
		if stat == nil {
			stat = &basicStats{}
			workspaceStats[request.requestType] = stat
		}
		stat.NewPoint(int64(delta))

		delete(ext.outstandingRequests, msg.ReqId)
	}
}

func (ext *extWorkspaceStats) publish() []Measurement {
	measurements := make([]Measurement, 0)

	for workspace, stats := range ext.stats {
		for requestType, stat := range stats {
			tags := make([]quantumfs.Tag, 0, 10)
			tags = appendNewTag(tags, "statName", ext.Name)
			tags = appendNewTag(tags, "operation", requestType)

			fields := make([]quantumfs.Field, 0, 10)
			fields = appendNewFieldString(fields, "workspace", workspace)
			fields = appendNewFieldInt(fields, "average_ns",
				stat.Average())
			fields = appendNewFieldInt(fields, "maximum_ns", stat.Max())
			fields = appendNewFieldInt(fields, "samples", stat.Count())

			for name, data := range stat.Percentiles() {
				fields = appendNewFieldInt(fields, name, data)
			}

			measurements = append(measurements, Measurement{
				name:   "quantumFsLatency",
				tags:   tags,
				fields: fields,
			})

			delete(ext.stats, workspace)
		}
	}

	return measurements
}

func (ext *extWorkspaceStats) gc() {
	for reqId, request := range ext.newRequests {
		if ext.AgedOut(request.lastUpdateGeneration) {
			fmt.Printf("%s: Deleting stale newRequest %d (%d/%d)\n",
				ext.Name, reqId, request.lastUpdateGeneration,
				ext.CurrentGeneration)
			delete(ext.newRequests, reqId)
		}
	}

	for reqId, request := range ext.outstandingRequests {
		if ext.AgedOut(request.lastUpdateGeneration) {
			fmt.Printf("%s: Deleting stale outstandingRequest %d "+
				"(%d/%d)\n", ext.Name, reqId,
				request.lastUpdateGeneration, ext.CurrentGeneration)
			delete(ext.outstandingRequests, reqId)
		}
	}
}
