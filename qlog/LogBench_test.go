// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qlog

// Test the logging subsystem performance

import (
	"runtime"
	"strings"
	"testing"

	"github.com/aristanetworks/quantumfs/utils"
)

var a uint32
var b uint32
var c uint32
var d string

func init() {
	a = 12345
	b = 67890
	c = 191919342
	d = "0xDEADBEEF"
}

func tmpDir() string {
	testPc, _, _, _ := runtime.Caller(1)
	testName := runtime.FuncForPC(testPc).Name()
	lastSlash := strings.LastIndex(testName, "/")
	return "/tmp/" + testName[lastSlash+1:]
}

func BenchmarkBigStdLog(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	qlog.SetLogLevels("Daemon/*")
	for i := 0; i < test.N; i++ {
		// It turns out that the time it takes golang to put vars into args
		// is about 140ns
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d %s", a, b, d)
	}
}

func BenchmarkQuick0Params(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, !!!!!!!!")
	}
}

func BenchmarkQuick1Params(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d !!!!!", a)
	}
}

func BenchmarkQuick2Params(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d !!", a, b)
	}
}

func BenchmarkQuick3Params(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d %d", a, b, c)
	}
}

func BenchmarkQuickString(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %s", d)
	}
}

func BenchmarkBigLogArgsArray(test *testing.B) {
	// This benchmark is to illustrate that most of the time consumed is in
	// golang taking variadic params and sticking them into the args... slice
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	var args []interface{}
	args = append(args, a)
	args = append(args, b)
	args = append(args, d)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d %s", args...)
	}
}

func BenchmarkBigLog(test *testing.B) {
	qlog, err := NewQlog(tmpDir())
	utils.AssertNoErr(err)

	for i := 0; i < test.N; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d %s", a, b, d)
	}
}
