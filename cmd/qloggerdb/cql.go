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
		qlogstats.NewExtPointStats(backends.CqlTtlCacheHit,
			"cql_setcache_hit"),
		qlogstats.NewExtPointStats(backends.CqlTtlCacheMiss,
			"cql_setcache_miss"),
		qlogstats.NewExtPointStats(backends.CqlTtlCacheEvict,
			"cql_setcache_evict"),

		// Data store latency
		newQfsExtPair(backends.CqlGetLog,
			backends.KeyLog),
		newQfsExtPair(backends.CqlSetLog,
			backends.KeyLog),

		// Workspace DB latency
		newQfsExtPair(backends.CqlTypespaceLog, ""),
		newQfsExtPair(backends.CqlNamespaceLog,
			backends.CqlNamespaceDebugLog),
		newQfsExtPair(backends.CqlWorkspaceListLog,
			backends.CqlWorkspaceListDebugLog),
		newQfsExtPair(backends.CqlWorkspaceLog,
			backends.CqlWorkspaceDebugLog),
		newQfsExtPair(backends.CqlBranchLog,
			backends.CqlBranchDebugLog),
		newQfsExtPair(backends.CqlAdvanceLog,
			backends.CqlAdvanceDebugLog),

		// CQL internal statistics
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
