// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	qutils "github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
	"github.com/aristanetworks/qubit/tools/qwalker/utils"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk = iota
	exitBadConfig
	exitMiscError
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

func getPrefixMatcher(prefix string, negate bool) func(s string) bool {
	if negate {
		return func(s string) bool {
			return !strings.HasPrefix(s, prefix)
		}
	}
	return func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}
}

func main() {

	walkFlags = flag.NewFlagSet("Walker daemon", flag.ExitOnError)

	etherCfg := walkFlags.String("etherCfg", "",
		"datastore and workspacedb config file")
	wsdbCfg := walkFlags.String("wsdbCfg", "", "workspacedb config")
	logdir := walkFlags.String("logdir", "", "dir for logging")
	influxServer := walkFlags.String("influxServer", "", "influxdb server's IP")
	influxPort := walkFlags.Uint("influxPort", 0, "influxdb server's port")
	influxDBName := walkFlags.String("influxDBName", "", "database to use in influxdb")
	numWalkers := walkFlags.Int("numWalkers", maxNumWalkers,
		"Number of parallel walks in the daemon")
	useSkipMap := walkFlags.Bool("skipSeenKeys", false, "Skip keys seen in earlier walks")
	name := walkFlags.String("name", "", "walker instance name, used when multiple instances are deployed")
	prefix := walkFlags.String("wsPrefixMatch", "", "all workspace names with this prefix are handled. Others"+
		" are skipped. The negWsPrefixMatch option reverses the check")
	negPrefixMatch := walkFlags.Bool("negWsPrefixMatch", false, "negate the prefix match checking. All workspaces"+
		" with names not matching the prefix are handled.")
	lastWriteDuration := walkFlags.String("skipWsWrittenWithin", "", "CAUTION: Since this option causes certain"+
		" workspaces to be skipped, use this option carefully. For example, using it with transient workspaces"+
		" is ok. This option causes walker daemon to skip walking workspaces which were written"+
		" within the given duration. The duration string is in Golang duration format. If the duration is empty"+
		" then the duration check is not used.")

	walkFlags.Usage = func() {
		fmt.Println("qubit-walkerd version", version)
		fmt.Println("usage: qubit-walkerd -name <name> -etherCfg <etherCfg> [-wsdbCfg <wsdb service name>] [-logdir dir]")
		fmt.Println("                [-influxServer serverIP -influxPort port" +
			" -influxDBName dbname] [-numWalkers num] [-skipSeenKeys]" +
			" [-wsPrefixMatch prefix] [-negWsPrefixMatch]" +
			" [-skipWsWrittenWithin duration]")
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

	if *name == "" {
		fmt.Println("Every walker daemon instance must have a name")
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	matcher := getPrefixMatcher(*prefix, *negPrefixMatch)

	if *etherCfg == "" {
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

	c := getWalkerDaemonContext(*name, *influxServer, uint16(*influxPort), *influxDBName,
		*etherCfg, *wsdbCfg, *logdir, *numWalkers, matcher, *lastWriteDuration)

	// Start heart beat messaging.
	timer := time.Tick(heartBeatInterval)
	go heartBeat(c, timer)
	qutils.ServePprof()

	walkFullWSDBLoop(c, true, *useSkipMap)
}

const SkipMapClearLog = "SkipMap period over - clearing."

func walkFullWSDBLoop(c *Ctx, backOffLoop bool, useSkipMap bool) {
	ttlCfg := c.ttlCfg
	if ttlCfg.SkipMapMaxLen == 0 {
		// Set a reasonable default value
		ttlCfg.SkipMapMaxLen = 20 * 1024 * 1024
	}

	var skipMapPeriod time.Duration
	var nextMapReset time.Time
	if useSkipMap {
		c.skipMap = utils.NewSkipMap(ttlCfg.SkipMapMaxLen)
		skipMapPeriod = time.Duration(ttlCfg.SkipMapResetAfter_ms) *
			time.Millisecond
		nextMapReset = time.Now().Add(skipMapPeriod)
	}

	for {
		c.iteration++

		atomic.StoreUint32(&c.numSuccess, 0)
		atomic.StoreUint32(&c.numError, 0)

		if useSkipMap && time.Now().After(nextMapReset) {
			c.vlog(SkipMapClearLog)
			c.skipMap.Clear()
			nextMapReset = time.Now().Add(skipMapPeriod)
		}

		startTimeOuter := time.Now()
		c.vlog("Iteration[%d] started at %s", c.iteration,
			startTimeOuter.String())

		err := walkFullWSDBSetup(c)

		dur := time.Since(startTimeOuter)
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		c.vlog("Iteration[%d] ended at %s took %s numError %d (err %s)",
			c.iteration, time.Now().String(), dur.String(), c.numError,
			errStr)
		AddPointWalkerIteration(c, dur)

		// If the walk iteration completes very quickly
		// then we can relax a bit before moving onto
		// the next iteration. This helps reduce unnecessary
		// load on the system (including stats generation).
		// If there are errors, then it makes sense to try
		// after sometime in the hope that errors are being
		// watched and operator has resolved the failure.
		if backOffLoop {
			backOff(c, dur)
		}
	}
}

// Each call to walkFullWSDBSetup will create a group context
// under which the following will be run
// - All the workers: They do the call to the walk library.
// - walkFullWSDB Inner: This queues work on the workChan
//
// When an error occurs in any of the gouroutines, c.Done() is triggered and
// all the goroutines exit. Errors returned from Walk() are
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

// skipWsWrittenWithin returns true if the workspace was written
// recently and so it should be skipped from walk.
func skipWsWrittenWithin(c *Ctx, ts, ns, ws string) bool {
	if c.lwDuration == 0 {
		return false
	}

	lastWriteTime, err := c.wsLastWriteTime(c, ts, ns, ws)
	if err != nil {
		c.elog("Error from WorkspaceLastWriteTime(%s/%s/%s): %v",
			ts, ns, ws, err.Error())
		return false
	}
	if time.Now().Sub(lastWriteTime) < c.lwDuration {
		return true
	}
	return false
}

// walkFullWSDB will iterate through all the TS/NS/WS once.
// It is possible for NamespaceList and WorkspaceList to be empty
// due to concurrent deletes from pruner. The TypespaceList will
// never be empty due to null typespace.
func walkFullWSDB(c *Ctx, workChan chan *workerData) error {

	tsl, err := c.wsdb.TypespaceList(c.qctx)
	if err != nil {
		c.elog("TypespaceList failed: %s", err.Error())
		atomic.AddUint32(&c.numError, 1)
		return err
	}
	exitNoRestart(c, "Typespace list should not be empty", len(tsl) != 0)

	for _, ts := range tsl {
		nsl, err := c.wsdb.NamespaceList(c.qctx, ts)
		if err != nil {
			c.elog("NamespaceList(%s) failed: %s", ts, err.Error())
			continue
		}
		for _, ns := range nsl {
			wsMap, err := c.wsdb.WorkspaceList(c.qctx, ts, ns)
			if err != nil {
				c.elog("WorkspaceList(%s/%s) failed: %s ",
					ts, ns, err.Error())
				continue
			}
			for ws := range wsMap {
				wsFullPath := fmt.Sprintf("%s/%s/%s", ts, ns, ws)
				if !c.wsNameMatcher(wsFullPath) {
					c.vlog("skipped walk of ws %s due to ws name match rule",
						wsFullPath)
					continue
				}

				if skipWsWrittenWithin(c, ts, ns, ws) {
					c.vlog("skipped walk of ws %s since ws was written within last %s",
						wsFullPath, c.lwDuration)
					continue
				}

				if err := queueWorkspace(c, workChan, ts, ns, ws); err != nil {
					c.elog("Error from queueWorkspace (%s/%s/%s): %s",
						ts, ns, ws, err.Error())
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
		c.elog("queueWorkspace received Done:%s. Did not queue %s/%s/%s",
			c.Err().Error(), t, n, w)
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
			c.elog("walkWorker[%d] received Done:%s", workerID,
				err.Error())
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
	if rootID, _, err = qubitutils.GetWorkspaceRootID(c.qctx, c.wsdb,
		wsname); err != nil {
		// walker daemon is asynchronous to pruner daemon and so
		// it is possible that pruner daemon removed the workspace
		// from workspaceDB in between walker daemon seeing it in
		// the list of workspaces and starting the walk of the
		// workspace.
		if werr, ok := err.(quantumfs.WorkspaceDbErr); ok &&
			werr.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
			c.wlog("%s/%s/%s not found, might have been pruned", ts, ns, ws)
			return nil
		}
		c.elog("Get rootID for %s/%s/%s, err(%s)", ts, ns, ws,
			err.Error())
		return err
	}

	w := wsDetails{
		ts:     ts,
		ns:     ns,
		ws:     ws,
		rootID: rootID.String(),
	}

	// Use a local SkipMap to hold keys we visit during a single workspace's walk.
	// If the walk fails we do not merge these keys with the the global SkipMap.
	var localSkipMap *utils.SkipMap
	if c.skipMap != nil {
		localSkipMap = utils.NewSkipMap(oldC.ttlCfg.SkipMapMaxLen)
	}

	// Every call to Walk() needs a walkFunc
	walkFunc := func(cw *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, objType quantumfs.ObjectType) error {

		return utils.RefreshTTL(cw, path, key, size, objType, c.cqlds,
			c.ttlCfg.TTLNew, c.ttlCfg.SkipMapResetAfter_ms/1000,
			c.skipMap, localSkipMap)
	}

	// Call the walker library.
	c.vlog("%s TTL refresh for %s/%s/%s (%s)", startPrefix, ts, ns, ws,
		rootID.String())
	if err = walker.Walk(c.qctx, c.ds, rootID, walkFunc); err != nil {
		c.elog("TTL refresh for %s/%s/%s (%s), err(%s)", ts, ns, ws,
			rootID.String(), err.Error())

		AddPointWalkerWorkspace(c, w, false, time.Since(start), err.Error())
	} else {
		if c.skipMap != nil {
			_, beforeSkipMapLen := c.skipMap.Len()
			_, localSkipMapLen := localSkipMap.Len()
			c.skipMap.Merge(localSkipMap)
			_, afterSkipMapLen := c.skipMap.Len()
			c.vlog("Merging localSkipMap(len=%d) with globalSkipMap(len=%d) "+
				"-> globalSkipMap(len=%d)",
				localSkipMapLen, beforeSkipMapLen, afterSkipMapLen)
		}
		c.vlog("%s TTL refresh for %s/%s/%s (%s)", successPrefix, ts, ns, ws,
			rootID.String())
		AddPointWalkerWorkspace(c, w, true, time.Since(start), "")
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
		c.vlog("Iteration[%d] ended with errors so sleep for %s until %s",
			c.iteration, backOffAfterErrors.String(),
			time.Now().Add(backOffAfterErrors).String())
		time.Sleep(backOffAfterErrors)
	case iterDur <= shortIterationDuration:
		c.vlog("Iteration[%d] took <= %s so sleep for %s until %s",
			c.iteration, shortIterationDuration.String(),
			backOffAfterShortIteration.String(),
			time.Now().Add(backOffAfterShortIteration).String())
		time.Sleep(backOffAfterShortIteration)
	default:
		c.vlog("Iteration[%d] took > %s so proceeding to next iteration",
			c.iteration, shortIterationDuration.String())
		return
	}
}
