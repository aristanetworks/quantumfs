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

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
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

func Log(c *quantumfs.Ctx, code int, format string, args ...interface{}) {
	format = format + "\n"

	switch code {
	case logErr:
		c.Elog(qlog.LogTool, format, args...)
		fmt.Printf("ERROR:"+format, args...)

	case logWarn:
		c.Wlog(qlog.LogTool, format, args...)
		fmt.Printf("WARN:"+format, args...)
	case logDebug:
		c.Dlog(qlog.LogTool, format, args...)
		fmt.Printf("DEBUG:"+format, args...)
	case logInfo:
		fallthrough
	default:
		c.Vlog(qlog.LogTool, format, args...)
		fmt.Printf("INFO:"+format, args...)
	}

}

var confFile string
var walkRerunPeriod = 4 * time.Hour

func main() {

	walkFlags = flag.NewFlagSet("Walker daemon", flag.ExitOnError)

	config := walkFlags.String("cfg", "", "datastore and workspaceDB config file")
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
	confFile = *config

	c := newCtx(*logdir)
	db, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", *config)
	if err != nil {
		Log(c, logErr, "Connection to workspaceDB failed\n")
		os.Exit(exitBadConfig)
	}

	var lastWalkStart time.Time
	var lastWalkDuration time.Duration
	for {

		lastWalkDuration = time.Since(lastWalkStart)
		if lastWalkDuration < walkRerunPeriod {

			Log(c, logInfo, "Not enough time has passed since last walk\n")
			sleepDur := time.Until(lastWalkStart.Add(walkRerunPeriod))
			Log(c, logInfo, "Sleeping for %v\n", sleepDur)
			time.Sleep(sleepDur)

		}
		lastWalkStart = time.Now()
		Log(c, logInfo, "Walk started at %v\n", lastWalkStart)
		walkFullWSDB(c, db)
		lastWalkDuration = time.Since(lastWalkStart)
		Log(c, logInfo, "Walk ended at %v took %v\n", time.Now(), lastWalkDuration)

	}
}

func walkFullWSDB(c *quantumfs.Ctx, db quantumfs.WorkspaceDB) {

	tsl, err := db.TypespaceList(c)
	if err != nil {
		Log(c, logErr, "Error in geting List of Typespaces")
	}
	for _, ts := range tsl {
		nsl, err := db.NamespaceList(c, ts)
		if err != nil {
			Log(c, logErr, "Error in geting List of Namespaces for TS:%s", ts)
		}
		for _, ns := range nsl {
			wsl, err := db.WorkspaceList(c, ts, ns)
			if err != nil {
				Log(c, logErr, "Error in geting List of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
			}
			for _, ws := range wsl {

				Log(c, logInfo, "Starting walk of %s/%s/%s\n",
					ts, ns, ws)
				stdStr, errStr, err := runCommand(c, ts, ns, ws)
				if err != nil || errStr != "" {
					Log(c, logErr, "Updating TTL for %s/%s/%s failed\n"+
						"STDOUT: %s"+"STDERR: %s",
						ts, ns, ws, stdStr, errStr)
				}
				Log(c, logInfo, "Finished walk of %s/%s/%s\n"+
					"STDOUT: %s", ts, ns, ws, stdStr)

			}
		}
	}
}

func runCommand(c *quantumfs.Ctx, ts string, ns string,
	ws string) (string, string, error) {

	fullWsName := ts + "/" + ns + "/" + ws

	cmd := exec.Command("/usr/sbin/qubit-walkercmd", "-cfg", confFile, "ttl", fullWsName)
	var stdOut bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &errOut
	err := cmd.Run()

	fmt.Printf("")
	return stdOut.String(), errOut.String(), err
}

func newCtx(logdir string) *quantumfs.Ctx {
	log := qlog.NewQlogTiny()
	if logdir != "" {
		log = qlog.NewQlog(logdir)
	}

	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: 1,
	}
	return c
}
