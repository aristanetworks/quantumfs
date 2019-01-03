// +build ether

// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/qlogstats"
)

func init() {
	ses := []qlogstats.StatExtractor{
		qlogstats.NewExtPointStats(cql.EtherTtlCacheHit,
			"ether_setcache_hit"),
		qlogstats.NewExtPointStats(cql.EtherTtlCacheMiss,
			"ether_setcache_miss"),
		qlogstats.NewExtPointStats(cql.EtherTtlCacheEvict,
			"ether_setcache_evict"),

		// Data store latency
		newQfsExtPair(cql.EtherGetLog,
			cql.KeyLog),
		newQfsExtPair(cql.EtherSetLog,
			cql.KeyLog),

		// Workspace DB latency
		newQfsExtPair(cql.EtherTypespaceLog, ""),
		newQfsExtPair(cql.EtherNamespaceLog,
			cql.EtherNamespaceDebugLog),
		newQfsExtPair(cql.EtherWorkspaceListLog,
			cql.EtherWorkspaceListDebugLog),
		newQfsExtPair(cql.EtherWorkspaceLog,
			cql.EtherWorkspaceDebugLog),
		newQfsExtPair(cql.EtherBranchLog,
			cql.EtherBranchDebugLog),
		newQfsExtPair(cql.EtherAdvanceLog,
			cql.EtherAdvanceDebugLog),

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
