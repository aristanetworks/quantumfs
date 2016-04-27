// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem

import "testing"
import "strings"
import "arista.com/quantumfs/testutils"

func TestLogSet_test(t *testing.T) {
	// let's redirect the log writer in qfs
	var logs string
	write = testutils.IoPipe(&logs)

	loadLevels("Daemon|2")
	Log(LogDaemon, MaxReqId, 1, "TestToken1")
	if !strings.Contains(logs, "TestToken1") {
		t.Fatal("Enabled log doesn't show up")
	}
	Log(LogDaemon, MaxReqId, 0, "TestToken0")
	if strings.Contains(logs, "TestToken0") {
		t.Fatal("Log level 0 not disabled by mask")
	}
	Log(LogWorkspacedb, MaxReqId, 0, "TestToken2")
	if !strings.Contains(logs, "TestToken2") {
		t.Fatal("Different subsystem erroneously affected by log setting")
	}
	
	loadLevels("")
	logs = ""
	for i := 1; i < int(maxLogLevels); i++ {
		Log(LogDaemon, MaxReqId, uint8(i), "TestToken")
		if strings.Contains(logs, "TestToken") {
			t.Fatal("Disabled log appeared")
		}
	}
	Log(LogDaemon, MaxReqId, 0, "TestToken")
	if !strings.Contains(logs, "TestToken") {
		t.Fatal("Default log level not working")
	}

	// Test variable arguments
	a := 12345
	b := 98765
	Log(LogDaemon, MaxReqId, 0, "Testing args %d %d", a, b)
	if !strings.Contains(logs, "12345") ||
		!strings.Contains(logs, "98765") {
		t.Fatal("Variable insertion in logs not working.")
	}
}

func TestLoadLevels_test(t *testing.T) {
	loadLevels("Daemon/*")
	if logLevels != 287 {
		t.Fatal("Wildcard log levels incorrectly set", logLevels)
	}

	// test out of order, combo setting, and general bitmask
	loadLevels("Daemon/1,WorkspaceDb/*,Datastore|10")
	if logLevels != 4003 {
		t.Fatal("Out of order, combo setting, or general bitmask broken",
			logLevels)
	}

	// test misspelling ignores misspelt entry. Ensure case insensitivity
	loadLevels("DaeMAN/1,WORKSPACEDB/*,Datastored|10")
	if logLevels != 3857 {
		t.Fatal("Case insensitivity broken / mis-spelling not ignored",
			logLevels)
	}
}
