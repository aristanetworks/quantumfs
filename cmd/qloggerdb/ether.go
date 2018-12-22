// +build ether

// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/qlogstats"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
)

func init() {
	ses := []qlogstats.StatExtractor{
		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheHit,
			"ether_setcache_hit"),
		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheMiss,
			"ether_setcache_miss"),
		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheEvict,
			"ether_setcache_evict"),

		// Data store latency
		newQfsExtPair(thirdparty_backends.EtherGetLog,
			thirdparty_backends.KeyLog),
		newQfsExtPair(thirdparty_backends.EtherSetLog,
			thirdparty_backends.KeyLog),

		// Workspace DB latency
		newQfsExtPair(thirdparty_backends.EtherTypespaceLog, ""),
		newQfsExtPair(thirdparty_backends.EtherNamespaceLog,
			thirdparty_backends.EtherNamespaceDebugLog),
		newQfsExtPair(thirdparty_backends.EtherWorkspaceListLog,
			thirdparty_backends.EtherWorkspaceListDebugLog),
		newQfsExtPair(thirdparty_backends.EtherWorkspaceLog,
			thirdparty_backends.EtherWorkspaceDebugLog),
		newQfsExtPair(thirdparty_backends.EtherBranchLog,
			thirdparty_backends.EtherBranchDebugLog),
		newQfsExtPair(thirdparty_backends.EtherAdvanceLog,
			thirdparty_backends.EtherAdvanceDebugLog),

		// Ether.CQL internal statistics
		newQfsExtPair(cql.DeleteLog, cql.KeyLog),
		newQfsExtPair(cql.GetLog, cql.KeyLog),
		newQfsExtPair(cql.InsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.UpdateLog, cql.KeyLog),
		newQfsExtPair(cql.MetadataLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlGetLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlInsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.GoCqlMetadataLog, cql.KeyLog),
	}

	extractors = append(extractors, ses...)
}
