// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"time"
)

//Semaphore implements semaphore using chan
type Semaphore chan int

const semTimeout time.Duration = 60 * time.Second

// P acquires a resources
func (s Semaphore) P() {

	timer := time.NewTimer(semTimeout)
	defer timer.Stop()

	select {
	case s <- 1:
	case <-timer.C:
		panic(fmt.Sprintf("Timeout in Semaphore.P() after %v of waiting",
			semTimeout))
	}
}

// V releases a resources
func (s Semaphore) V() {
	timer := time.NewTimer(semTimeout)
	defer timer.Stop()

	select {
	case <-s:
	case <-timer.C:
		panic(fmt.Sprintf("Timeout in Semaphore.V() after %v of waiting",
			semTimeout))
	}
}

// HumanizeBytes returns a string representing the size
// suffixed with human readable units like B=bytes, KB=kilobytes etc
func HumanizeBytes(size uint64) string {

	suffix := []string{"B", "KB", "MB", "GB"}

	f := float64(size)
	var i int
	for i = 0; f >= 1024 && i < len(suffix); i++ {
		f = f / 1024
	}

	if i == len(suffix) {
		i = i - 1
	}

	return fmt.Sprintf("%.1f %s", f, suffix[i])
}
