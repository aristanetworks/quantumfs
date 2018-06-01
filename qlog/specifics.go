// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains specifics that shouldn't be part of qlog, but aren't nicely
// factored out for the time being.

const (
	LogDaemon LogSubsystem = iota
	LogDatastore
	LogWorkspaceDb
	LogTest
	LogQlog
	LogQuark
	LogSpin
	LogTool
	logSubsystemMax = LogTool
)

const (
	MuxReqId uint64 = math.MaxUint64 - iota
	FlushReqId
	QlogReqId
	TestReqId
	RefreshReqId
	MinFixedReqId
)

const (
	MinSpecialReqId     = uint64(0xb) << 48
	FlusherRequestIdMin = uint64(0xb) << 48
	RefreshRequestIdMin = uint64(0xc) << 48
	ForgetRequstIdMin   = uint64(0xd) << 48
	UnusedRequestIdMin  = uint64(0xe) << 48
)

func specialReq(reqId uint64) string {
	if reqId > MinFixedReqId {
		switch reqId {
		default:
			return "UNKNOWN"
		case MuxReqId:
			return "[Mux]"
		case FlushReqId:
			return "[Flush]"
		case QlogReqId:
			return "[Qlog]"
		case TestReqId:
			return "[Test]"
		case RefreshReqId:
			return "[Refresh]"
		}
	}

	var format string
	var offset uint64
	switch {
	default:
		format = "%d"
		offset = 0
	case FlusherRequestIdMin <= reqId && reqId < RefreshRequestIdMin:
		format = "[Flush%d]"
		offset = FlusherRequestIdMin
	case RefreshRequestIdMin <= reqId && reqId < ForgetRequstIdMin:
		format = "[Refresh%d]"
		offset = RefreshRequestIdMin
	case ForgetRequstIdMin <= reqId && reqId < UnusedRequestIdMin:
		format = "[Forget%d]"
		offset = ForgetRequstIdMin
	}

	return fmt.Sprintf(format, reqId-offset)
}

var logSubsystemList = []logSubsystemPair{
	{"Daemon", LogDaemon},
	{"Datastore", LogDatastore},
	{"WorkspaceDb", LogWorkspaceDb},
	{"Test", LogTest},
	{"Qlog", LogQlog},
	{"Quark", LogQuark},
	{"Spin", LogSpin},
	{"Tool", LogTool},
}
