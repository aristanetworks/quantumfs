// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
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

	walkFlags.Usage = func() {
		fmt.Println("This daemon periodically walks all the workspaces ")
		fmt.Println("and updates the TTL of each block as per the config file")

		fmt.Println("usage: qwalkerd [config]")
		walkFlags.PrintDefaults()
	}

	walkFlags.Parse(os.Args[1:])

	if *config == "" {
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	c := getWalkerDaemonContext("test", *config, *logdir)

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

	tsl, err := c.wsdb.TypespaceList(c.qctx)
	if err != nil {
		c.elog("Error in geting List of Typespaces")
	}
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
				c.vlog("Starting walk of %s/%s/%s", ts, ns, ws)
				runCommand(c, ts, ns, ws)
			}
		}
	}
}

func runCommand(c *Ctx, ts string, ns string, ws string) {

	fullWsName := ts + "/" + ns + "/" + ws

	cmd := exec.Command("/usr/sbin/qubit-walkercmd", "-cfg", c.confFile,
		"ttl", fullWsName)
	var stdOut bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &errOut
	err := cmd.Run()

	if err != nil || errOut.String() != "" {
		c.elog("Updating TTL for %s/%s/%s failed", ts, ns, ws)
		c.elog("STDOUT: %s", stdOut.String())
		c.elog("STDERR: %s", errOut.String())
	} else {
		c.vlog("Updating TTL for %s/%s/%s successful", ts, ns, ws)
		c.vlog("STDOUT: %s", stdOut.String())
	}
}
