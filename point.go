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
//	   errStr      - error String when the walk is failed.
///
func AddPointWalkerWorkspace(c *Ctx, w wsDetails, pass bool,
	dur time.Duration, walkErr string) {

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
		"errStr":    walkErr,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("InfluxDB %s=%s %s/%s/%s (%s) iteration=%d "+
			"walkSuccess=%v walkErrMsg=(%q) InfluxErr:%s\n",
			measurement, dur.String(), w.ts, w.ns, w.ws, w.rootID, c.iteration,
			pass, walkErr, err.Error())
		return
	}
	c.vlog("%s InfluxDB %s=%s "+"%s/%s/%s (%s) iteration=%d walkSuccess=%v "+
		"walkErrMsg=%s \n",
		successPrefix, measurement, dur.String(), w.ts, w.ns, w.ws, w.rootID, c.iteration,
		pass, walkErr)
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
		c.elog("InfluxDB %s=%s iteration=%d numSuccess=%d numError=%d "+
			"InfluxErr: %s\n",
			measurement, dur.String(), c.iteration, c.numSuccess, c.numError,
			err.Error())
		return
	}
	c.vlog("%s InfluxDB %s=%s iteration=%d numSuccess=%d numError=%d\n",
		successPrefix, measurement, dur.String(), c.iteration, c.numSuccess, c.numError)
}

// Write point to indicate that walker is alive.
//
// tags:   keyspace    - Keyspace of the WorkspaceDB
//
// fields: alive      - a monotonically increasing count.
//         skipMapLen - Num keys in the skipMap
//         skipLRULen - Num elements in skipMap LRU
//
func AddPointWalkerHeartBeat(c *Ctx) {

	if c.Influx == nil {
		return
	}
	c.aliveCount += 1
	skipLRULen, skipMapLen := c.skipMap.Len()

	measurement := "walkerHeartBeat"
	tags := map[string]string{
		"keyspace": c.keyspace,
	}
	fields := map[string]interface{}{
		"alive":        c.aliveCount,
		"iteration":    c.iteration,
		"countSuccess": c.numSuccess,
		"countError":   c.numError,
		"skipMapLen":   skipMapLen,
		"skipLRULen":   skipLRULen,
	}

	err := c.Influx.WritePoint(measurement, tags, fields)
	if err != nil {
		c.elog("InfluxDB %s aliveCount=%d iteration=%d numSuccess=%d numError=%d "+
			"skipMapLen=%d skipLRULen=%d InfluxErr=%s\n",
			successPrefix, measurement, c.aliveCount, c.iteration, c.numSuccess, c.numError,
			skipMapLen, skipLRULen, err.Error())
		return
	}
	c.vlog("%s InfluxDB %s aliveCount=%d iteration=%d numSuccess=%d numError=%d "+
		"skipMapLen=%d skipLRULen=%d\n",
		successPrefix, measurement, c.aliveCount, c.iteration, c.numSuccess, c.numError,
		skipMapLen, skipLRULen)
}
