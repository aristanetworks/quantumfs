// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"strconv"
	"time"

	"github.com/aristanetworks/quantumfs"
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
//         host      - Hostname of the machine running the walker
//         name      - Name given to the walker instance
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

	measurement := "walkerWorkspace"
	tags := make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("typeSpace", w.ts))
	tags = append(tags, quantumfs.NewTag("nameSpace", w.ns))
	tags = append(tags, quantumfs.NewTag("pass", strconv.FormatBool(pass)))
	tags = append(tags, quantumfs.NewTag("keyspace", c.keyspace))
	tags = append(tags, quantumfs.NewTag("host", c.host))
	tags = append(tags, quantumfs.NewTag("name", c.name))

	fields := make([]quantumfs.Field, 0)
	fields = append(fields, quantumfs.NewFieldString("workSpace", w.ws))
	fields = append(fields, quantumfs.NewFieldInt("walkTime",
		int64(dur/time.Second)))
	fields = append(fields, quantumfs.NewFieldInt("iteration",
		int64(c.iteration)))
	fields = append(fields, quantumfs.NewFieldString("rootID", w.rootID))
	fields = append(fields, quantumfs.NewFieldString("errStr", walkErr))

	c.WriteStatPoint(measurement, tags, fields)
	c.vlog("%s InfluxDB %s=%s "+"%s/%s/%s (%s) iteration=%d walkSuccess=%v "+
		"walkErrMsg=%s \n",
		successPrefix, measurement, dur.String(), w.ts, w.ns, w.ws, w.rootID,
		c.iteration, pass, walkErr)
}

// AddPointWalkerIteration is a measurement point writer
//
// tags:   keyspace     - Keyspace of the WorkspaceDB
//         host         - Hostname of the machine running the walker
//         name         - Name given to the walker instance
//
// fields: walkTime     - Time it took to walk all the workspace in
//                        seconds (uint)
//         iteration    - The iteration number for this walk
//         countSuccess - Num successful walks
//         countError   - Num failed walks
//
func AddPointWalkerIteration(c *Ctx, dur time.Duration) {
	measurement := "walkerIteration"
	tags := make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("keyspace", c.keyspace))
	tags = append(tags, quantumfs.NewTag("host", c.host))
	tags = append(tags, quantumfs.NewTag("name", c.name))

	fields := make([]quantumfs.Field, 0)
	fields = append(fields, quantumfs.NewFieldInt("walkTime",
		int64(dur/time.Second)))
	fields = append(fields, quantumfs.NewFieldInt("iteration",
		int64(c.iteration)))
	fields = append(fields, quantumfs.NewFieldInt("countSuccess",
		int64(c.numSuccess)))
	fields = append(fields, quantumfs.NewFieldInt("countError",
		int64(c.numError)))

	c.WriteStatPoint(measurement, tags, fields)
	c.vlog("%s InfluxDB %s=%s iteration=%d numSuccess=%d numError=%d\n",
		successPrefix, measurement, dur.String(), c.iteration, c.numSuccess,
		c.numError)
}

// Write point to indicate that walker is alive.
//
// tags:   keyspace   - Keyspace of the WorkspaceDB
//         host       - Hostname of the machine running the walker
//         name       - Name given to the walker instance
//
// fields: alive      - a monotonically increasing count.
//         skipMapLen - Num keys in the skipMap
//         skipLRULen - Num elements in skipMap LRU
//
func AddPointWalkerHeartBeat(c *Ctx) {
	c.aliveCount += 1
	var skipLRULen, skipMapLen int
	if c.skipMap != nil {
		skipLRULen, skipMapLen = c.skipMap.Len()
	}

	measurement := "walkerHeartBeat"
	tags := make([]quantumfs.Tag, 0)
	tags = append(tags, quantumfs.NewTag("keyspace", c.keyspace))
	tags = append(tags, quantumfs.NewTag("host", c.host))
	tags = append(tags, quantumfs.NewTag("name", c.name))

	fields := make([]quantumfs.Field, 0)
	fields = append(fields, quantumfs.NewFieldInt("alive", c.aliveCount))
	fields = append(fields, quantumfs.NewFieldInt("iteration",
		int64(c.iteration)))
	fields = append(fields, quantumfs.NewFieldInt("countSuccess",
		int64(c.numSuccess)))
	fields = append(fields, quantumfs.NewFieldInt("countError",
		int64(c.numError)))
	fields = append(fields, quantumfs.NewFieldInt("skipMapLen",
		int64(skipMapLen)))
	fields = append(fields, quantumfs.NewFieldInt("skipLRULen",
		int64(skipLRULen)))

	c.WriteStatPoint(measurement, tags, fields)
	c.vlog("%s InfluxDB %s aliveCount=%d iteration=%d numSuccess=%d "+
		"numError=%d skipMapLen=%d skipLRULen=%d\n",
		successPrefix, measurement, c.aliveCount, c.iteration, c.numSuccess,
		c.numError, skipMapLen, skipLRULen)
}
