// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package inmem implements in-memory statistics managers
package inmem

import (
	"fmt"
	"sync"
	"time"

	hist "github.com/VividCortex/gohistogram"
	"github.com/aristanetworks/ether/utils/stats"
)

type opStatsInMem struct {
	name          string
	mutex         sync.RWMutex
	hist          hist.Histogram
	totalOps      int64
	firstStatTime time.Time
	lastStatTime  time.Time
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
	// when multiple concurrent callers record
	// operations or stats, to calculate operations
	// per second, we should know the time period
	// between first recorded stat and last recorded
	// stat
	if s.firstStatTime.Equal(time.Time{}) {
		s.firstStatTime = time.Now()
	}

	s.lastStatTime = time.Now()
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
	dur := s.lastStatTime.Sub(s.firstStatTime)
	if secs := int64(dur.Seconds()); secs != 0 {
		fmt.Printf("  %d ops/sec\n", s.totalOps/secs)
	} else {
		fmt.Printf("  Operations: %d Nanoseconds: %d\n",
			s.totalOps, dur.Nanoseconds())
	}
}
