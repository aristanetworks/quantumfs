// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package stats provides statistics management helpers
package stats

import "time"

// A variety of statistics managers can be implemented.
// Eg: inmemory, influx etc
// Each of these managers must implement OpStats. They
// can optionally implement OpStatReporter

// OpStats provides APIs for capturing statistics
// like latency, operations/sec etc
type OpStats interface {
	RecordOp(latency time.Duration)
}

// OpStatReporter reports the operation statistics
// like latency, rate etc to stdout
type OpStatReporter interface {
	ReportOpStats()
}
