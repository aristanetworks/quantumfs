// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the Low memory mode marker file

import (
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func TestLowMemExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		lowmemName := workspace + "/" + quantumfs.LowMemFileName

		var stat syscall.Stat_t
		test.AssertErr(syscall.Stat(lowmemName, &stat))

		test.qfs.inLowMemoryMode = true

		test.WaitFor("lowmem file to appear", func() bool {
			var stat syscall.Stat_t
			err := syscall.Stat(lowmemName, &stat)
			return err == nil
		})
	})
}
