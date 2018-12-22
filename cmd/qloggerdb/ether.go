// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/qlogstats"
)

func init() {
	ses := []qlogstats.StatExtractor{
		qlogstats.NewExtPointStats(backends.EtherTtlCacheHit,
			"ether_setcache_hit"),
		qlogstats.NewExtPointStats(backends.EtherTtlCacheMiss,
			"ether_setcache_miss"),
		qlogstats.NewExtPointStats(backends.EtherTtlCacheEvict,
			"ether_setcache_evict"),

		// Data store latency
		newQfsExtPair(backends.EtherGetLog,
			backends.KeyLog),
		newQfsExtPair(backends.EtherSetLog,
			backends.KeyLog),

		// Workspace DB latency
		newQfsExtPair(backends.EtherTypespaceLog, ""),
		newQfsExtPair(backends.EtherNamespaceLog,
			backends.EtherNamespaceDebugLog),
		newQfsExtPair(backends.EtherWorkspaceListLog,
			backends.EtherWorkspaceListDebugLog),
		newQfsExtPair(backends.EtherWorkspaceLog,
			backends.EtherWorkspaceDebugLog),
		newQfsExtPair(backends.EtherBranchLog,
			backends.EtherBranchDebugLog),
		newQfsExtPair(backends.EtherAdvanceLog,
			backends.EtherAdvanceDebugLog),

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
