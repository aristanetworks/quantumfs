// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

// SigHandler contains information to stop the signal handler
type SigHandler struct {
	ch  chan os.Signal
	wg  *sync.WaitGroup
	sig os.Signal
}

// StartSigHandler starts a goroutine which runs the
// handler when sig signal is received. This routine
// does not support using the same signal more than once
// within the same program
func StartSigHandler(handler func(), sig os.Signal) *SigHandler {

	sigH := SigHandler{
		ch:  make(chan os.Signal, 1),
		wg:  &sync.WaitGroup{},
		sig: sig,
	}

	sigH.wg.Add(1)

	go func() {
		defer sigH.wg.Done()
		signal.Notify(sigH.ch, sig)
		for {
			s := <-sigH.ch

			// when main is exiting
			if s != sig {
				break
			}
			handler()
		}
	}()

	return &sigH
}

// StopSigHandler stops the signal handler
func StopSigHandler(sh *SigHandler) {
	signal.Stop(sh.ch)
	close(sh.ch)
	sh.wg.Wait()
}

// StartStackTraceSigHandler starts a signal handler which
// which dumps stack of all goroutines when SIGUSR1 is received
func StartStackTraceSigHandler() *SigHandler {

	buf := make([]byte, 10*(1<<20))
	f := func() {
		stacklen := runtime.Stack(buf, true)
		fmt.Printf("\n=== All goroutine stack trace ===\n")
		fmt.Printf("%s\n", buf[:stacklen])
		fmt.Printf("\n=== stack trace end ===\n")
	}

	return StartSigHandler(f, syscall.SIGUSR1)
}
