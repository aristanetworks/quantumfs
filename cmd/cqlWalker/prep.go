// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/aristanetworks/ether"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

// Ctx implements both quantumfs.Ctx and ether.Ctx
type Ctx struct {
	quantumfs.Ctx
}

func (c *Ctx) Elog(fmtStr string, args ...interface{}) {
	c.Ctx.Elog(qlog.LogWorkspaceDb, fmtStr, args...)
}

func (c *Ctx) Wlog(fmtStr string, args ...interface{}) {
	c.Ctx.Wlog(qlog.LogWorkspaceDb, fmtStr, args...)
}

func (c *Ctx) Dlog(fmtStr string, args ...interface{}) {
	c.Ctx.Dlog(qlog.LogWorkspaceDb, fmtStr, args...)
}

func (c *Ctx) Vlog(fmtStr string, args ...interface{}) {
	c.Ctx.Vlog(qlog.LogWorkspaceDb, fmtStr, args...)
}

func (c *Ctx) FuncIn(funcName string, fmtStr string,
	args ...interface{}) ether.FuncOut {

	el := c.Ctx.FuncIn(qlog.LogWorkspaceDb, funcName,
		fmtStr, args...)
	return (ether.FuncOut)(el)
}

func (c *Ctx) FuncInName(funcName string) ether.FuncOut {
	return c.FuncIn(funcName, "")
}

func newCtx() *Ctx {
	var c Ctx
	// Create  Ctx with random RequestId
	logger, err := qlog.NewQlog("")
	if err != nil {
		fmt.Printf("Error in initializing NewQlog: %v\n", err)
		os.Exit(1)
	}
	requestID := uint64(1)
	c.Ctx = quantumfs.Ctx{
		Qlog:      logger,
		RequestId: requestID,
	}

	return &c
}

func showProgress(progress bool, start time.Time, keysWalked uint64) {

	if progress {
		fmt.Printf("\rTime: %-20v Keys Walked: %v",
			time.Since(start), keysWalked)
	}
}

func walkHelper(c *Ctx,
	qfsds quantumfs.DataStore,
	qfsdb quantumfs.WorkspaceDB,
	ws string,
	progress bool,
	rootIDStatus bool,
	handler walker.WalkFunc) (err error) {

	var rootID quantumfs.ObjectKey

	// treat ws as rootID first and if that
	// fails then assume it is a name
	rootID, err = quantumfs.FromString(ws)
	if err != nil {
		if rootID, _, err = qubitutils.GetWorkspaceRootID(&c.Ctx,
			qfsdb, ws); err != nil {
			return
		}
	}

	if rootIDStatus {
		defer func() {
			if err != nil {
				err = fmt.Errorf("Walk failed at rootID: %s error: %v",
					rootID.String(), err)
			} else {
				fmt.Printf("Walk success at rootID: %s\n",
					rootID.String())
			}
		}()
	}

	wrapper := handler
	if progress {
		// add a wrapper around handler to show progress
		start := time.Now()
		var keysWalked uint64
		wrapper = func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
			size uint64, objType quantumfs.ObjectType) error {

			atomic.AddUint64(&keysWalked, 1)
			defer showProgress(progress, start, keysWalked)
			return handler(c, path, key, size, objType)
		}

		// add a newline to separate the progress information
		// and handler information
		defer fmt.Println()
	}

	if err = walker.Walk(&c.Ctx, qfsds, rootID, wrapper); err != nil {
		return err
	}

	return nil
}

// getTrackerHandler returns a handler that can walk a workspace and uses
// the filter function to decide if information should be collected for a path.
// The tracker object where information is collected is also returned.
func getTrackerHandler(filter func(path string) bool) (*tracker, walker.WalkFunc) {

	tracker := newTracker()
	var mapLock utils.DeferableMutex
	handler := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, objType quantumfs.ObjectType) error {

		defer mapLock.Lock().Unlock()
		if filter != nil && filter(path) {
			return nil
		}
		tracker.addKey(hex.EncodeToString(key.Value()),
			path, size)
		return nil
	}

	return tracker, handler
}
