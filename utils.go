// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package utils package is for generic blobs of codes used in ether.
package utils

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
		panic(fmt.Sprintf("Timeout in Semaphore.P() after %v of waiting", semTimeout))
	}
}

// V releases a resources
func (s Semaphore) V() {
	timer := time.NewTimer(semTimeout)
	defer timer.Stop()

	select {
	case <-s:
	case <-timer.C:
		panic(fmt.Sprintf("Timeout in Semaphore.V() after %v of waiting", semTimeout))
	}
}
