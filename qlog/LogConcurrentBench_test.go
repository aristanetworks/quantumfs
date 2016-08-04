// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem performance

import "testing"
import "sync"


func DoBench(qlog *Qlog, n int, wg *sync.WaitGroup) {
	
	var args []interface{}
	args = append(args, a)
	args = append(args, b)
	args = append(args, d)

	for i := 0; i < n; i++ {
		qlog.Log(LogDaemon, MuxReqId, 1,
			"TestToken with a few params, %d %d %s", args...)
	}

	wg.Done()
}

func ConcurrentLog(cores int, test *testing.B) {
	qlog := NewQlog("")
	var wg sync.WaitGroup

	for i := 0; i < cores; i++ {
		wg.Add(1)
		go DoBench(qlog, test.N, &wg)
	}

	wg.Wait()
}

func BenchmarkConcurrent1(test *testing.B) {
	ConcurrentLog(1, test)
}

func BenchmarkConcurrent2(test *testing.B) {
	ConcurrentLog(2, test)
}

func BenchmarkConcurrent4(test *testing.B) {
	ConcurrentLog(4, test)
}

func BenchmarkConcurrent8(test *testing.B) {
	ConcurrentLog(8, test)
}

func BenchmarkConcurrent16(test *testing.B) {
	ConcurrentLog(16, test)
}

func BenchmarkConcurrent32(test *testing.B) {
	ConcurrentLog(32, test)
}

func BenchmarkConcurrent64(test *testing.B) {
	ConcurrentLog(64, test)
}
