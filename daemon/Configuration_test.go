// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test various QuantumFS and FUSE configuration knobs

import "fmt"
import "io/ioutil"
import "strings"
import "testing"

import "github.com/aristanetworks/quantumfs"

// Confirm the bid configuration knobs have been set correctly.
func TestBdiKnobs(t *testing.T) {
	mbs := fmt.Sprintf("%d\n", quantumfs.MaxBlockSize/1024)
	runTest(t, func(test *TestHelper) {
		read_ahead_kb := fmt.Sprintf("/sys/class/bdi/0:%d/read_ahead_kb",
			test.fuseConnection)
		max_ratio := fmt.Sprintf("/sys/class/bdi/0:%d/max_ratio",
			test.fuseConnection)

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
