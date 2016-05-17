// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "arista.com/quantumfs/qlog"

// Generic request context object
type Ctx struct {
	Qlog      *qlog.Qlog
	RequestId uint64
}

func (c Ctx) Elog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 0, format, args...)
}

func (c Ctx) Wlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 1, format, args...)
}

func (c Ctx) Dlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 2, format, args...)
}

func (c Ctx) Vlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 3, format, args...)
}
