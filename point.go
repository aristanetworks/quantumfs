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

	measurement := "workspaceWalkDuration"
	tags := map[string]string{
		"typeSpace": ts,
		"nameSpace": ns,
		"pass":      strconv.FormatBool(pass),
	}
	fields := map[string]interface{}{
		"workSpace":   ws,
		"walkTimeSec": uint(dur / time.Second),
		"iteration":   c.iteration,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Writing %s to influxDB for "+
			"%s/%s/%s iteration=%v walkSuccess=%v err:%v\n",
			measurement, ts, ns, ws, c.iteration, pass, err)
		return
	}
	c.vlog("%s Writing %s=%v to influxDB for "+
		"%s/%s/%s iteration=%v walkSuccess=%v \n",
		successPrefix, measurement, dur, ts, ns, ws, c.iteration, pass)
}

// WriteWalkerIteration is a measurement point writer
//
// tags:   none
//
// fields: walkTimeMin - Time it took to walk all the workspace in
//                       minutes (uint)
//         countSuccess - Num successful walks
//         countError   - Num failed walks
//
func WriteWalkerIteration(c *Ctx, dur time.Duration,
	numSuccess uint32, numError uint32) {

	measurement := "walkerIteration"
	tags := map[string]string{}
	fields := map[string]interface{}{
		"walkTimeMin":  uint(dur / time.Minute),
		"iteration":    c.iteration,
		"countSuccess": numSuccess,
		"countError":   numError,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Writing %s iteration=%v to influxDB err: %v\n",
			measurement, c.iteration, err)
		return
	}
	c.vlog("%s Writing %s=%v iteration=%v numSuccess=%v numError=%v to influxDB\n",
		successPrefix, measurement, dur, c.iteration, numSuccess, numError)
}
