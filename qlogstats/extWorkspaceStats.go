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

type accumulatingStats struct {
	stats                map[string]*basicStats // ie. ["Mux::Read"]
	lastUpdateGeneration uint64
}

type extWorkspaceStats struct {
	StatExtractorBaseWithGC

	lock                utils.DeferableMutex
	newRequests         map[uint64]newRequest
	outstandingRequests map[uint64]outstandingRequest

	accumulatingStats map[string]*accumulatingStats // ie. [workspace]
	// ie [workspace]["Mux::Read"]
	finishedStats map[string]map[string]*basicStats
}

func NewExtWorkspaceStats(nametag string, operations []string) StatExtractor {
	ext := &extWorkspaceStats{
		newRequests:         make(map[uint64]newRequest),
		outstandingRequests: make(map[uint64]outstandingRequest),
		accumulatingStats:   make(map[string]*accumulatingStats),
		finishedStats:       make(map[string]map[string]*basicStats),
	}

	operations = append(operations, daemon.FuseRequestWorkspace)
	operations = append(operations, daemon.WorkspaceFinishedFormat)

	ext.StatExtractorBaseWithGC = NewStatExtractorBaseWithGC(nametag, ext,
		OnPartialFormat, operations)

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

	case msg.Format == daemon.FuseRequestWorkspace+"\n":
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

		workspaceStats, exists := ext.accumulatingStats[request.workspace]
		if !exists {
			workspaceStats = &accumulatingStats{
				stats: make(map[string]*basicStats),
			}
			ext.accumulatingStats[request.workspace] = workspaceStats
		}

		stat := workspaceStats.stats[request.requestType]
		if stat == nil {
			stat = &basicStats{}
			workspaceStats.stats[request.requestType] = stat
		}
		stat.NewPoint(int64(delta))
		workspaceStats.lastUpdateGeneration = ext.CurrentGeneration

		delete(ext.outstandingRequests, msg.ReqId)

	case strings.Compare(msg.Format, daemon.WorkspaceFinishedFormat+"\n") == 0:
		// The user claims they are done with the workspace, aggregate the
		// statistics for this workspace for upload.

		workspace, ok := msg.Args[0].(string)
		if !ok {
			fmt.Printf("%s: Argument not string: %v\n", ext.Name,
				msg.Args[0])
			return
		}

		stats, exists := ext.accumulatingStats[workspace]
		if !exists {
			fmt.Printf("%s: finishing unstarted workspace '%s'\n",
				ext.Name, workspace)
			return
		}
		ext.finishedStats[workspace] = stats.stats
		delete(ext.accumulatingStats, workspace)
	}
}

func (ext *extWorkspaceStats) publish() []Measurement {
	measurements := make([]Measurement, 0)

	for workspace, stats := range ext.finishedStats {
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

			delete(ext.finishedStats, workspace)
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

	for workspace, wsStat := range ext.accumulatingStats {
		if ext.AgedOut(wsStat.lastUpdateGeneration) {
			fmt.Printf("%s: Deleting stale wsStats %s "+
				"(%d/%d)\n", ext.Name, workspace,
				wsStat.lastUpdateGeneration, ext.CurrentGeneration)
			delete(ext.accumulatingStats, workspace)
		}
	}
}
