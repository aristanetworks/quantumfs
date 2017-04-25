// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "github.com/aristanetworks/quantumfs/qlog"

// Generic request context object
type Ctx struct {
	Qlog      *qlog.Qlog
	RequestId uint64
}

// Log an Error message
func (c Ctx) Elog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 0, format, args...)
}

// Log a Warning message
func (c Ctx) Wlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 1, format, args...)
}

// Log a Debug message
func (c Ctx) Dlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 2, format, args...)
}

// Log a Verbose tracing message
func (c Ctx) Vlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 3, format, args...)
}
