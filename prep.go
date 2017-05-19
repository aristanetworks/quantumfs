// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
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
		fmt.Printf("\r %10v %v %20v %v",
			"Time", time.Since(start), "Keys Walked", keysWalked)
	}
}
