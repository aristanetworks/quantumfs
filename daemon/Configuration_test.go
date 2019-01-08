// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test various QuantumFS and FUSE configuration knobs

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/aristanetworks/quantumfs"
)

// Confirm the bid configuration knobs have been set correctly.
func TestBdiKnobs(t *testing.T) {
	mbs := fmt.Sprintf("%d\n", quantumfs.MaxBlockSize/1024)
	runTest(t, func(test *testHelper) {
		read_ahead_kb := fmt.Sprintf("/sys/class/bdi/0:%d/read_ahead_kb",
			test.fuseConnections[0])
		max_ratio := fmt.Sprintf("/sys/class/bdi/0:%d/max_ratio",
			test.fuseConnections[0])

		test.WaitFor("sysfs files to be written", func() bool {
			readAhead, err := ioutil.ReadFile(read_ahead_kb)
			if err != nil {
				return false
			}

			maxRatio, err := ioutil.ReadFile(max_ratio)
			if err != nil {
				return false
			}

			if strings.Compare(string(readAhead), mbs) == 0 &&
				strings.Compare(string(maxRatio), "100\n") == 0 {

				return true
			}
			return false
		})
	})
}
