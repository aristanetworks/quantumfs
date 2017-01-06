// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// APIStatsReporter reports statistics like latency, rate etc
//  when the statistics are maintained in memory rather than
//  external statistics stores (eg: InfluxDB)
//  This is an optional interface
type APIStatsReporter interface {
	ReportAPIStats()
}
