// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"strconv"
	"time"
)

// WriteWorkspaceWalkDuration is a measurement point writer
// tags:   typeSpace - Typespace for the quantumfs workspace
//         nameSpace - Namespace for the quantumfs workspace
//         pass      - Walked failed or passed
//
// fields: workSpace - Name of the workspace (text)
//         walkTimeSec - Time it took to walk the workspace in
//                       seconds (uint)
///
func WriteWorkspaceWalkDuration(c *Ctx, ts string, ns string, pass bool,
	ws string, dur time.Duration) {

	tags := map[string]string{
		"typeSpace": ts,
		"nameSpace": ns,
		"pass":      strconv.FormatBool(pass),
	}
	fields := map[string]interface{}{
		"workSpace":   ws,
		"walkTimeSec": uint(dur / time.Second),
	}
	measurement := "workspaceWalkDuration"

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Error writing %s to influxDB for "+
			"%s/%s/%s walkSuccess=%v err:%v\n", measurement, ts, ns, ws, pass, err)
		return
	}
	c.vlog("Success writing %s=%v to influxDB for "+
		"%s/%s/%s walkSuccess=%v \n", measurement, dur, ts, ns, ws, pass)
}

// WriteWalkerStride is a measurement point writer
//
// tags:   none
//
// fields: walkTimeMin - Time it took to walk all the workspace in
//                       minutes (uint)
//         countSuccess - Num successful walks
//         countError   - Num failed walks
//
func WriteWalkerStride(c *Ctx, dur time.Duration, numSuccess uint,
	numError uint) {

	tags := map[string]string{}
	fields := map[string]interface{}{
		"walkTimeMin":  uint(dur / time.Minute),
		"countSuccess": numSuccess,
		"countError":   numError,
	}
	measurement := "walkerStride"

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Error writing %s to influxDB err: %v\n", measurement, err)
		return
	}
	c.vlog("Success writing %s=%v numSuccess=%v numError=%v to influxDB\n",
		measurement, dur, numSuccess, numError)
}
