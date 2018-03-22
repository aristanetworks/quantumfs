// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
)

// Start an http pprof instance on an available port.
func ServePprof() {
	go servePprof()
}

func servePprof() {
	var listener net.Listener

	port := 6060
	for {
		addr := fmt.Sprintf("localhost:%d", port)
		l, err := net.Listen("tcp", addr)

		if err == nil {
			fmt.Printf("Serving pprof on port %d\n", port)
			listener = l
			break
		}
		port++
	}

	err := http.Serve(listener, nil)
	if err != nil {
		fmt.Printf("net-pprof error: %v\n", err)
	}
}
