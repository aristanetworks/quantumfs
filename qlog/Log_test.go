// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem

import "testing"
import "strings"
import "github.com/aristanetworks/quantumfs/testutils"

func TestLogSet_test(t *testing.T) {
	qlog := NewQlog("")
	// let's redirect the log writer in qfs
	var logs string
	qlog.SetWriter(testutils.IoPipe(&logs))

	qlog.SetLogLevels("Daemon|2")
	qlog.Log(LogDaemon, MuxReqId, 1, "TestToken1")
	if !strings.Contains(logs, "TestToken1") {
		t.Fatal("Enabled log doesn't show up")
	}
	qlog.Log(LogDaemon, MuxReqId, 0, "TestToken0")
	if strings.Contains(logs, "TestToken0") {
		t.Fatal("Log level 0 not disabled by mask")
	}
	qlog.Log(LogWorkspaceDb, MuxReqId, 0, "TestToken2")
	if !strings.Contains(logs, "TestToken2") {
		t.Fatal("Different subsystem erroneously affected by log setting")
	}

	qlog.SetLogLevels("")
	logs = ""
	for i := 1; i < int(maxLogLevels); i++ {
		qlog.Log(LogDaemon, MuxReqId, uint8(i), "TestToken")
		if strings.Contains(logs, "TestToken") {
			t.Fatal("Disabled log appeared")
		}
	}
	qlog.Log(LogDaemon, MuxReqId, 0, "TestToken")
	if !strings.Contains(logs, "TestToken") {
		t.Fatal("Default log level not working")
	}

	// Test variable arguments
	a := 12345
	b := 98765
	qlog.Log(LogDaemon, MuxReqId, 0, "Testing args %d %d", a, b)
	if !strings.Contains(logs, "12345") ||
		!strings.Contains(logs, "98765") {
		t.Fatal("Variable insertion in logs not working.")
	}
}

func TestLoadLevels_test(t *testing.T) {
	qlog := NewQlog("")

	qlog.SetLogLevels("Daemon/*")
	if qlog.logLevels != 0x111F {
		t.Fatalf("Wildcard log levels incorrectly set: %x != %x", 0x111f,
			qlog.logLevels)
	}

	// test out of order, combo setting, and general bitmask
	qlog.SetLogLevels("Daemon/1,WorkspaceDb/*,Datastore|10")
	if qlog.logLevels != 0x1FA3 {
		t.Fatalf("Out of order, combo setting, or general bitmask broken %x",
			qlog.logLevels)
	}

	// test misspelling ignores misspelt entry. Ensure case insensitivity
	qlog.SetLogLevels("DaeMAN/1,WORKSPACEDB/*,Datastored|10")
	if qlog.logLevels != 0x1F11 {
		t.Fatalf("Case insensitivity broken / mis-spelling not ignored %x",
			qlog.logLevels)
	}
}
