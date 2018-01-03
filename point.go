// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"strconv"
	"time"
)

type wsDetails struct {
	ts     string
	ns     string
	ws     string
	rootID string
}

// AddPointWalkerWorkspace is a measurement point writer
// tags:   typeSpace - Typespace for the quantumfs workspace
//         nameSpace - Namespace for the quantumfs workspace
//         pass      - Walk failed or passed
//         keyspace  - Keyspace of the WorkspaceDB
//
// fields: workSpace   - Name of the workspace (text)
//         walkTime    - Time it took to walk the workspace in
//                       seconds (uint)
//         iteration   - The iteration number for this walk
//         rootID      - rootID for the workSpace
///
func AddPointWalkerWorkspace(c *Ctx, w wsDetails, pass bool,
	dur time.Duration) {

	if c.Influx == nil {
		return
	}

	measurement := "walkerWorkspace"
	tags := map[string]string{
		"typeSpace": w.ts,
		"nameSpace": w.ns,
		"pass":      strconv.FormatBool(pass),
		"keyspace":  c.keyspace,
	}
	fields := map[string]interface{}{
		"workSpace": w.ws,
		"walkTime":  uint(dur / time.Second),
		"iteration": c.iteration,
		"rootID":    w.rootID,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Writing %s to influxDB for "+
			"%s/%s/%s (%s) iteration=%d walkSuccess=%v err:%s\n",
			measurement, w.ts, w.ns, w.ws, w.rootID,
			c.iteration, pass, err.Error())
		return
	}
	c.vlog("%s Writing %s=%s to influxDB for "+
		"%s/%s/%s (%s) iteration=%d walkSuccess=%v \n",
		successPrefix, measurement, dur.String(),
		w.ts, w.ns, w.ws, w.rootID,
		c.iteration, pass)
}

// AddPointWalkerIteration is a measurement point writer
//
// tags:   keyspace    - Keyspace of the WorkspaceDB
//
// fields: walkTime  - Time it took to walk all the workspace in
//                        seconds (uint)
//         iteration    - The iteration number for this walk
//         countSuccess - Num successful walks
//         countError   - Num failed walks
//
func AddPointWalkerIteration(c *Ctx, dur time.Duration) {

	if c.Influx == nil {
		return
	}

	measurement := "walkerIteration"
	tags := map[string]string{
		"keyspace": c.keyspace,
	}
	fields := map[string]interface{}{
		"walkTime":     uint(dur / time.Second),
		"iteration":    c.iteration,
		"countSuccess": c.numSuccess,
		"countError":   c.numError,
	}
	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Writing %s iteration=%d to influxDB err: %s\n",
			measurement, c.iteration, err.Error())
		return
	}
	c.vlog("%s Writing %s=%s iteration=%d numSuccess=%d numError=%d to influxDB\n",
		successPrefix, measurement, dur.String(), c.iteration, c.numSuccess,
		c.numError)
}

// Write point to indicate that walker is alive.
//
// tags:   keyspace    - Keyspace of the WorkspaceDB
//
// fields: alive - a place holder. Since, we have to have a field.
//
func AddPointWalkerHeartBeat(c *Ctx) {

	if c.Influx == nil {
		return
	}

	measurement := "walkerHeartBeat"
	tags := map[string]string{
		"keyspace": c.keyspace,
	}
	fields := map[string]interface{}{
		"alive":        1,
		"iteration":    c.iteration,
		"countSuccess": c.numSuccess,
		"countError":   c.numError,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("Error:   Writing %s to influxDB err: %s\n", measurement,
			err.Error())
		return
	}
	c.vlog("%s Writing %s iteration=%d numSuccess=%d numError=%d to influxDB\n",
		successPrefix, measurement, c.iteration, c.numSuccess, c.numError)
}
