// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package inmem implements in-memory statistics managers
package inmem

import (
	"fmt"
	hist "github.com/VividCortex/gohistogram"
	"github.com/aristanetworks/ether/utils/stats"
	"sync"
	"time"
)

type opStatsInMem struct {
	name          string
	mutex         sync.RWMutex
	hist          hist.Histogram
	totalOps      int64
	totalNanosecs int64
}

// NewOpStatsInMem creates an instance of in-memory
// operational statistics manager
func NewOpStatsInMem(name string) stats.OpStats {
	return &opStatsInMem{
		name: name,
		hist: hist.NewHistogram(100),
	}
}

func (s *opStatsInMem) RecordOp(latency time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.hist.Add(float64(latency.Nanoseconds()))
	s.totalOps++
	s.totalNanosecs += latency.Nanoseconds()
}

func (s *opStatsInMem) ReportOpStats() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	fmt.Printf("Operation stats for %q\n", s.name)
	fmt.Println(" Latency (usecs)")
	fmt.Printf("  25 percentile: %.2f\n", s.hist.Quantile(0.25)/1000)
	fmt.Printf("  50 percentile: %.2f\n", s.hist.Quantile(0.50)/1000)
	fmt.Printf("  75 percentile: %.2f\n", s.hist.Quantile(0.75)/1000)
	fmt.Printf("  99 percentile: %.2f\n", s.hist.Quantile(0.99)/1000)
	fmt.Printf("  99.9 percentile: %.2f\n", s.hist.Quantile(0.999)/1000)
	fmt.Println(" Rate")
	if secs := s.totalNanosecs / 1000000000; secs != 0 {
		fmt.Printf("  %d ops/sec\n", s.totalOps/secs)
	} else {
		fmt.Printf("  Operations: %d Nanoseconds: %d\n",
			s.totalOps, s.totalNanosecs)
	}
}
