// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "arista.com/quantumfs/qlog"
import "fmt"
import "strconv"
import "time"

func (c ctx) Log(idx qlog.LogSubsystem, level uint8, format string,
	args ...interface{}) {
	// todo: send to shared memory

	t := time.Now()

	if qlog.GetLogLevel(idx, level) {
		fmt.Printf(t.Format(time.StampNano) + " " + idx.String() + " " +
			strconv.FormatUint(c.requestId, 10) + ": " + format + "\n",
			args...)
	}
}

// local daemon package specific log wrappers
func (c ctx) elog(format string, args ...interface{}) {
	c.Log(qlog.LogDaemon, 0, format, args...)
}

func (c ctx) wlog(format string, args ...interface{}) {
	c.Log(qlog.LogDaemon, 1, format, args...)
}

func (c ctx) dlog(format string, args ...interface{}) {
	c.Log(qlog.LogDaemon, 2, format, args...)
}

func (c ctx) vlog(format string, args ...interface{}) {
	c.Log(qlog.LogDaemon, 3, format, args...)
}

