// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/walker"
	"github.com/aristanetworks/qubit/tools/qwalker/utils"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk        = iota
	exitBadConfig = iota
)

const maxNumWalkers = 4 // default num goroutines calling the actual walk lib.
const heartBeatInterval = 1 * time.Minute
const successPrefix = "Success:"
const eventPrefix = "Event:  "
const startPrefix = "Start:  "

const (
	logInfo  = iota
	logDebug = iota
	logWarn  = iota
	logErr   = iota
)

var walkFlags *flag.FlagSet
var version string

func (c *Ctx) vlog(format string, args ...interface{}) {
	c.qctx.Vlog(qlog.LogTool, format, args...)
}
func (c *Ctx) dlog(format string, args ...interface{}) {
	c.qctx.Dlog(qlog.LogTool, format, args...)
}
func (c *Ctx) wlog(format string, args ...interface{}) {
	c.qctx.Wlog(qlog.LogTool, format, args...)
}
func (c *Ctx) elog(format string, args ...interface{}) {
	c.qctx.Elog(qlog.LogTool, "Error:  "+format, args...)
}

func main() {

	walkFlags = flag.NewFlagSet("Walker daemon", flag.ExitOnError)

	etherCfg := walkFlags.String("ether", "",
		"datastore config file")
	wsdbCfg := walkFlags.String("wsdb", "",
		"hostname for workspaceDB service")
	logdir := walkFlags.String("logdir", "", "dir for logging")
	influxServer := walkFlags.String("influxServer", "", "influxdb server's IP")
	influxPort := walkFlags.Uint("influxPort", 0, "influxdb server's port")
	influxDBName := walkFlags.String("influxDBName", "", "database to use in influxdb")
	numWalkers := walkFlags.Int("numWalkers", maxNumWalkers,
		"Number of parallel walks in the daemon")

	walkFlags.Usage = func() {
		fmt.Println("qubit-walkerd version", version)
		fmt.Println("usage: qwalkerd -ether <config> -wsdb <config> [-logdir dir]")
		fmt.Println("                [-influxServer serverIP -influxPort port" +
			" -influxDBName dbname] [-numWalkers num]")
		fmt.Println()
		fmt.Println("This daemon periodically walks all the workspaces,")
		fmt.Println("updates the TTL of each block as per the config file and")
		fmt.Println("uploads the statistics to influx.")
		fmt.Println()
		walkFlags.PrintDefaults()
	}

	if err := walkFlags.Parse(os.Args[1:]); err != nil {
		fmt.Println("Parsing of flags failed err:", err)
		os.Exit(exitBadConfig)
	}

	if *etherCfg == "" || *wsdbCfg == "" {
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	// If influxServer is specified ensure than
	// ensure than influxDBName is also specified.
	if *influxServer != "" && (*influxPort == 0 || *influxDBName == "") {
		fmt.Println("When providing influxServer, influxPort ")
		fmt.Println("and influxDBName needs to be provided as well.")
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	c := getWalkerDaemonContext(*influxServer, uint16(*influxPort), *influxDBName,
		*etherCfg, *wsdbCfg, *logdir, *numWalkers)

	// Start heart beat messaging.
	timer := time.Tick(heartBeatInterval)
	go heartBeat(c, timer)

	walkFullWSDBLoop(c)
}

func walkFullWSDBLoop(c *Ctx) {
	for {
		c.iteration++

		atomic.StoreUint32(&c.numSuccess, 0)
		atomic.StoreUint32(&c.numError, 0)

		startTimeOuter := time.Now()
		c.vlog("Iteration[%v] started at %v", c.iteration, startTimeOuter)

		err := walkFullWSDBSetup(c)

		dur := time.Since(startTimeOuter)
		c.vlog("Iteration[%v] ended at %v took %v numError %d (err %v)",
			c.iteration, time.Now(), dur, c.numError, err)
		AddPointWalkerIteration(c, dur, c.numSuccess, c.numError)

		// If the walk iteration completes very quickly
		// then we can relax a bit before moving onto
		// the next iteration. This helps reduce unnecessary
		// load on the system (including stats generation).
		// If there are errors, then it makes sense to try
		// after sometime in the hope that errors are being
		// watched and operator has resolved the failure.
		backOff(c, dur)
	}
}

// Each call to walkFullWSDBSetup will create a group context
// under which the following will be run
// - All the workers: They do the call to the walk library.
// - walkFullWSDB Inner: This queues work on the workChan
//
// When an error occurs in any of the gouroutines, c.Done() is triggered and
// all the goroutines exit. Errors returned from walker.Walk() are
// ignored.
func walkFullWSDBSetup(c *Ctx) error {

	group, groupCtx := errgroup.WithContext(context.Background())
	c.Context = groupCtx
	workChan := make(chan *workerData)

	// Start the workers.
	for i := 1; i <= c.numWalkers; i++ {
		id := i
		group.Go(func() error {
			return walkWorker(c, workChan, id)
		})
	}

	// Start the walk of the wsdb.
	group.Go(func() error {
		defer close(workChan)
		return walkFullWSDB(c, workChan)
	})

	// For for everyone.
	return group.Wait()
}

// exitNoRestart causes the walker daemon to exit and since exit code is zero
// the daemon manager/launcher (eg: systemd) will not restart the daemon. The
// expectation is that operator will intervene since walker down alert will be
// raised.
func exitNoRestart(c *Ctx, exitMsg string, trueCond bool) {
	if !trueCond {
		c.elog("Daemon exiting due to: %s", exitMsg)
		os.Exit(0)
	}
}

// walkFullWSDB will iterate through all the TS/NS/WS once.
// It is possible for NamespaceList and WorkspaceList to be empty
// due to concurrent deletes from pruner. The TypespaceList will
// never be empty due to null typespace.
func walkFullWSDB(c *Ctx, workChan chan *workerData) error {

	tsl, err := c.wsdb.TypespaceList(c.qctx)
	if err != nil {
		c.elog("TypespaceList failed: %v", err)
		atomic.AddUint32(&c.numError, 1)
		return err
	}
	exitNoRestart(c, "Typespace list should not be empty", len(tsl) != 0)

	for _, ts := range tsl {
		nsl, err := c.wsdb.NamespaceList(c.qctx, ts)
		if err != nil {
			c.elog("NamespaceList(%s) failed: %v", ts, err)
			continue
		}
		for _, ns := range nsl {
			wsMap, err := c.wsdb.WorkspaceList(c.qctx, ts, ns)
			if err != nil {
				c.elog("WorkspaceList(%s/%s) failed: %v ",
					ts, ns, err)
				continue
			}
			for ws := range wsMap {
				if err := queueWorkspace(c, workChan, ts, ns, ws); err != nil {
					c.elog("Error from queueWorkspace (%s/%s/%s): %v",
						ts, ns, ws, err)
					return err
				}
			}
		}
	}
	return nil
}

type workerData struct {
	ts string
	ns string
	ws string
}

func queueWorkspace(c *Ctx, workChan chan<- *workerData, t string, n string,
	w string) error {
	select {
	case <-c.Done():
		c.elog("queueWorkspace received Done:%v. Did not queue %s/%s/%s",
			c.Err(), t, n, w)
		return c.Err()
	case workChan <- &workerData{ts: t, ns: n, ws: w}:
		c.vlog("%s Workspace queued %s/%s/%s", eventPrefix, t, n, w)

	}
	return nil
}

func walkWorker(c *Ctx, workChan <-chan *workerData, workerID int) (err error) {

	c.vlog("%s walkWorker[%d] started", eventPrefix, workerID)
	for {
		select {
		case <-c.Done():
			err = c.Err()
			c.elog("walkWorker[%d] received Done:%v", workerID, err)
			return
		case w := <-workChan:
			if w == nil {
				err = nil
				c.vlog("%s walkWorker[%d] received nil on workChan",
					eventPrefix, workerID)
				return
			}
			c.vlog("%s walkWorker[%d] processing %s/%s/%s",
				eventPrefix, workerID, w.ts, w.ns, w.ws)
			if cmdErr := runWalker(c, w.ts, w.ns, w.ws); cmdErr != nil {
				atomic.AddUint32(&c.numError, 1)
			} else {
				atomic.AddUint32(&c.numSuccess, 1)
			}
		}
	}
}

// Wrapper around the call to the walker library.
func runWalker(oldC *Ctx, ts string, ns string, ws string) error {
	var err error
	var rootID quantumfs.ObjectKey
	wsname := ts + "/" + ns + "/" + ws
	c := oldC.newRequestID() // So that each walk has its own ID in the qlog.

	start := time.Now()
	if rootID, _, err = qubitutils.GetWorkspaceRootID(c.qctx, c.wsdb, wsname); err != nil {
		return err
	}

	w := wsDetails{
		ts:     ts,
		ns:     ns,
		ws:     ws,
		rootID: rootID.String(),
	}

	// Every call to walker.Walk() needs a walkFunc
	walkFunc := func(cw *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		return utils.RefreshTTL(cw, path, key, size, isDir, c.cqlds,
			c.ttlCfg.TTLThreshold, c.ttlCfg.TTLNew)
	}

	// Call the walker library.
	c.vlog("%s TTL refresh for %s/%s/%s (%s)", startPrefix, ts, ns, ws, rootID.String())
	if err = walker.Walk(c.qctx, c.ds, rootID, walkFunc); err != nil {
		c.elog("TTL refresh for %s/%s/%s (%s), err(%v)", ts, ns, ws,
			rootID.String(), err)

		AddPointWalkerWorkspace(c, w, false, time.Since(start))
	} else {
		c.vlog("%s TTL refresh for %s/%s/%s (%s)", successPrefix, ts, ns, ws, rootID.String())
		AddPointWalkerWorkspace(c, w, true, time.Since(start))
	}
	return err
}

// Send out a heartbeat whenever the timer ticks.
func heartBeat(c *Ctx, timer <-chan time.Time) {
	AddPointWalkerHeartBeat(c)
	for {
		select {
		case <-timer:
			AddPointWalkerHeartBeat(c)
		}
	}
}

// backOff implements a simple policy as of now -
//
// If there are errors during walk iteration then
// we backoff for 30mins. Workspace errors and
// database errors are not differentiated.
// If there are no errors in walk iteration then
//   After walk iteration of <= 5min duration
//     we backoff for 10mins
//   After walk iteration of > 5min duration
//     we proceed to the next iteration without
//     any backoff
const backOffAfterErrors = 30 * time.Minute
const shortIterationDuration = 5 * time.Minute
const backOffAfterShortIteration = 10 * time.Minute

func backOff(c *Ctx, iterDur time.Duration) {

	switch {
	case c.numError != 0:
		c.vlog("Iteration[%v] ended with errors so sleep for %s until %v",
			c.iteration, backOffAfterErrors,
			time.Now().Add(backOffAfterErrors))
		time.Sleep(backOffAfterErrors)
	case iterDur <= shortIterationDuration:
		c.vlog("Iteration[%v] took <= %s so sleep for %s until %v",
			c.iteration, shortIterationDuration,
			backOffAfterShortIteration,
			time.Now().Add(backOffAfterShortIteration))
		time.Sleep(backOffAfterShortIteration)
	default:
		c.vlog("Iteration[%v] took > %s so proceeding to next iteration",
			c.iteration, shortIterationDuration)
		return
	}
}
