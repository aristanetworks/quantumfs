// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
	requestID := uint64(1)
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestID,
	}

	return ctx
}

func showProgress(progress bool, start time.Time, keysWalked uint64) {

	if progress {
		fmt.Printf("\rTime: %-20v Keys Walked: %v",
			time.Since(start), keysWalked)
	}
}

func walkHelper(c *quantumfs.Ctx,
	qfsds quantumfs.DataStore,
	qfsdb quantumfs.WorkspaceDB,
	wsname string,
	progress bool,
	rootIDStatus bool,
	handler walker.WalkFunc) (err error) {

	var rootID quantumfs.ObjectKey
	if rootID, _, err = qubitutils.GetWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return
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
			size uint64, isDir bool) error {

			atomic.AddUint64(&keysWalked, 1)
			defer showProgress(progress, start, keysWalked)
			return handler(c, path, key, size, isDir)
		}

		// add a newline to separate the progress information
		// and handler information
		defer fmt.Println()
	}

	if err = walker.Walk(c, qfsds, rootID, wrapper); err != nil {
		return err
	}

	return nil
}

// getTrackerHandler returns a handler that can walk a workspace and uses
// the filter function to decide if information should be collected for a path.
// The tracker object where information is collected is also returned.
func getTrackerHandler(collectDedupeInfo bool,
	filter func(path string) bool) (*tracker, walker.WalkFunc) {

	tracker := newTracker(collectDedupeInfo)
	var mapLock utils.DeferableMutex
	handler := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

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
