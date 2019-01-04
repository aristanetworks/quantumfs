// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"time"
)

// APIStatsReporter reports statistics like latency, rate etc
//  when the statistics are maintained in memory rather than
//  external statistics stores (eg: InfluxDB)
//  This is an optional interface
type APIStatsReporter interface {
	ReportAPIStats()
}

// ExtKeyInfo is extended CQL store specific
// information for a key
type ExtKeyInfo struct {
	TTL       int       // seconds of TTL
	WriteTime time.Time // last time key's value was written
}

// CqlStore interface is a collection of methods
// which are specific to a blobstore that supports
// CQL protocol.
type CqlStore interface {
	// Keyspace for the store
	Keyspace() string
	// Get extended information like TTL, WriteTime etc
	GetExtKeyInfo(c Ctx, key []byte) (ExtKeyInfo, error)
}
