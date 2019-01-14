// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
)

// Start an http pprof instance on an available port.
func ServePprof() {
	go servePprof()
}

func retryAfterError(err error) bool {
	prolog := "Unable to serve pprof: "
	oe, ok := err.(*net.OpError)
	if !ok {
		fmt.Printf(prolog+"Unknown error %#v\n", err)
		return false
	}

	se, ok := oe.Err.(*os.SyscallError)
	if !ok {
		fmt.Printf(prolog+"Unknown OpError %#v %#v\n", err, oe.Err)
		return false
	}

	if se.Err != syscall.EADDRINUSE {
		fmt.Printf(prolog+"Syscall error %#v %#v\n", err, se)
		return false
	}

	return true
}

func servePprof() {
	var listener net.Listener

	port := 6060
	for {
		addr := fmt.Sprintf("localhost:%d", port)
		l, err := net.Listen("tcp", addr)

		if err != nil {
			if retryAfterError(err) {
				port++
				continue
			} else {
				// retryAfterError() will have output a diagnostic
				return
			}
		}

		fmt.Printf("Serving pprof on port %d\n", port)
		listener = l
		break
	}

	err := http.Serve(listener, nil)
	if err != nil {
		fmt.Printf("net-pprof error: %v\n", err)
	}
}
