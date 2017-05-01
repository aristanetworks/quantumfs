// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aristanetworks/quantumfs/qlog"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk        = iota
	exitBadConfig = iota
)

const (
	logInfo  = iota
	logDebug = iota
	logWarn  = iota
	logErr   = iota
)

var walkFlags *flag.FlagSet

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
	c.qctx.Elog(qlog.LogTool, format, args...)
}

var walkRerunPeriod = 4 * time.Hour

func main() {

	walkFlags = flag.NewFlagSet("Walker daemon", flag.ExitOnError)

	config := walkFlags.String("cfg", "",
		"datastore and workspaceDB config file")
	logdir := walkFlags.String("logdir", "", "dir for logging")
	influxServer := walkFlags.String("influxServer", "", "influxdb server's IP:port")
	influxDB := walkFlags.String("influxDB", "", "database to use in influxdb")

	walkFlags.Usage = func() {
		fmt.Println("usage: qwalkerd -cfg <config> [-logdir dir] [-progress ] ")
		fmt.Println("                [-influxServer serverIP:port -influxDB dbname]")
		fmt.Println()
		fmt.Println("This daemon periodically walks all the workspaces,")
		fmt.Println("updates the TTL of each block as per the config file and")
		fmt.Println("uploads the statistics to influx.")
		fmt.Println()
		walkFlags.PrintDefaults()
	}

	walkFlags.Parse(os.Args[1:])

	if *config == "" {
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	// If influxServer is specified ensure than
	// ensure than influxDB is also specified.
	if *influxServer != "" && *influxDB == "" {
		fmt.Println("When providing influxServer, influxDB needs to be provided")
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	c := getWalkerDaemonContext(*influxServer, *config, *logdir, *influxDB)

	var lastWalkStart time.Time
	var lastWalkDuration time.Duration
	for {
		lastWalkDuration = time.Since(lastWalkStart)
		if lastWalkDuration < walkRerunPeriod {

			c.vlog("Not enough time has passed since last walk")
			sleepDur := time.Until(lastWalkStart.Add(walkRerunPeriod))
			c.vlog("Sleeping for %v", sleepDur)
			time.Sleep(sleepDur)

		}
		lastWalkStart = time.Now()
		c.vlog("Walk started at %v", lastWalkStart)
		walkFullWSDB(c)
		lastWalkDuration = time.Since(lastWalkStart)
		c.vlog("Walk ended at %v took %v", time.Now(),
			lastWalkDuration)
	}
}

// walkFullWSDB will iterate through all the TS/NS/WS once.
func walkFullWSDB(c *Ctx) {

	var numSuccess uint
	var numError uint
	tsl, err := c.wsdb.TypespaceList(c.qctx)
	if err != nil {
		c.elog("Error in geting List of Typespaces")
	}
	startTimeOuter := time.Now()
	for _, ts := range tsl {
		nsl, err := c.wsdb.NamespaceList(c.qctx, ts)
		if err != nil {
			c.elog("Error in geting List of Namespaces for TS:%s", ts)
		}
		for _, ns := range nsl {
			wsl, err := c.wsdb.WorkspaceList(c.qctx, ts, ns)
			if err != nil {
				c.elog("Error in geting List of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
			}
			for _, ws := range wsl {
				startTime := time.Now()
				err := runCommand(c, ts, ns, ws)
				if err != nil {
					numError++
					WriteWorkspaceWalkDuration(c, ts, ns, false, ws, time.Since(startTime))
				} else {
					numSuccess++
					WriteWorkspaceWalkDuration(c, ts, ns, true, ws, time.Since(startTime))
				}
				c.qctx.RequestId++
			}
		}
	}
	WriteWalkerStride(c, time.Since(startTimeOuter), numSuccess, numError)
}

func runCommand(c *Ctx, ts string, ns string, ws string) error {

	c.vlog("Updating TTL for %s/%s/%s started", ts, ns, ws)
	var cmdPath string
	cmdPath = "/usr/sbin/qubit-walkercmd"
	fullWsName := ts + "/" + ns + "/" + ws
	cmd := exec.Command(cmdPath, "-cfg", c.confFile,
		"ttl", fullWsName)

	var stdOut bytes.Buffer
	cmd.Stdout = &stdOut
	err := cmd.Run()

	outStr := strings.Replace(stdOut.String(), "\n", " ", -1)
	if err != nil {
		c.elog("Updating TTL for %s/%s/%s failed", ts, ns, ws)
		c.elog("%s", outStr)
		return err
	}
	c.vlog("Updating TTL for %s/%s/%s finished", ts, ns, ws)
	c.vlog("%s", outStr)
	return nil
}
