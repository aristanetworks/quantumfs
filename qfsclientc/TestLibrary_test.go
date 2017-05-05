// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import "flag"
import "os"
import "testing"

import "github.com/aristanetworks/quantumfs/daemon"

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}

func runTest(t *testing.T, test func(test *daemon.TestHelper)) {
	t.Parallel()
	daemon.RunTestCommon(t, daemon.QuantumFsTestCast(test), true, nil)
}
