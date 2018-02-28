// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build gocql

package cql

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

// The benchmarks here are used primarily to spot deviations
// in performance of GoCQL APIs when used with our cluster
// configuration and schemas. Intentionally, the write and
// read routines do not use Ether code but instead use GoCQL
// APIs directly.

// These benchmarks do not reflect typical Ether usage and
// hence do not use the results to advertize Ether performance.
// These benchmarks can also help during performance investigations
// where GoCQL baseline performance information is needed.

// globals
// gcp prefix = Go CQL performance
var gcpKeyspace, gcpBlobstore, gcpWsdb string

func init() {

	prefix := os.Getenv("CFNAME_PREFIX")
	gcpBlobstore = fmt.Sprintf("%sblobstore", prefix)
	gcpWsdb = fmt.Sprintf("%sworkspacedb", prefix)
}

func setupCluster(cfg ClusterConfig) *gocql.ClusterConfig {
	c := NewRealCluster(cfg)
	rc, _ := c.(*RealCluster)

	return rc.cluster
}

// use GoCQL APIs to benchmark wsdb writes
func benchWsdbWrites(b *testing.B, cluster *gocql.ClusterConfig, ttl int) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	// ttl is unused
	qryStr := fmt.Sprintf("insert into %s.%s("+
		"typespace, namespace, workspace, key, nonce, "+
		"publishtime, immutable) values (?,?,?,?,?,?,?)",
		gcpKeyspace, gcpWsdb)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query(qryStr, "wsdb", "bench", "test", nil,
				0, 0, false).Exec()
			if err != nil {
				b.Fatal(err)
				return
			}
		}
	})
}

// use GoCQL APIs to benchmark wsdb cached reads
func benchWsdbCachedReads(b *testing.B, cluster *gocql.ClusterConfig,
	ttl int) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	// setup the data to be read
	// ttl is unused
	wQryStr := fmt.Sprintf("insert into %s.%s("+
		"typespace, namespace, workspace, key, nonce, "+
		"publishtime, immutable) values (?,?,?,?,?,?,?)",
		gcpKeyspace, gcpWsdb)
	err = db.Query(wQryStr, "wsdb", "bench", "test", nil, 0, 0, false).Exec()
	if err != nil {
		b.Fatal(err)
		return
	}

	// must use the index in the table else the query takes too long
	// since all the nodes are checked for data
	rQryStr := fmt.Sprintf("select typespace, namespace, workspace, key, "+
		"nonce, publishtime, immutable "+
		"from %s.%s where typespace='wsdb' and namespace='bench' "+
		"and workspace='test'", gcpKeyspace, gcpWsdb)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var typespace, workspace, namespace string
		var nonce, publishtime int64
		var immutable bool
		var key []byte

		for pb.Next() {
			err := db.Query(rQryStr).Scan(&typespace, &namespace, &workspace, &key,
				&nonce, &publishtime, &immutable)
			if err != nil {
				b.Fatal(err)
				return
			}
			if typespace != "wsdb" || namespace != "bench" ||
				workspace != "test" || key != nil {
				b.Fatalf("Unexpected data: %s/%s/%s %s",
					typespace, namespace, workspace, key)
				return
			}
		}
	})
}

// use GoCQL APIs to benchmark blob cached reads
func benchBlobCachedReads(b *testing.B, cluster *gocql.ClusterConfig,
	key string, check []byte, writeBuf []byte, ttl int) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	var wQryStr string
	if ttl < 0 {
		wQryStr = fmt.Sprintf("insert into %s.%s (key, value)"+
			"values (?,?)", gcpKeyspace, gcpBlobstore)
	} else {
		wQryStr = fmt.Sprintf("insert into %s.%s (key, value)"+
			"values (?,?) using ttl %d", gcpKeyspace, gcpBlobstore, ttl)
	}

	err = db.Query(wQryStr, []byte(key), writeBuf).Exec()
	if err != nil {
		b.Fatal(err)
		return
	}

	// must use the index in the table else the query takes too long
	// since all the nodes are checked for data
	var rQryStr string
	if ttl < 0 {
		rQryStr = fmt.Sprintf("select value from %s.%s "+
			"where key = ?", gcpKeyspace, gcpBlobstore)
	} else {
		rQryStr = fmt.Sprintf("select value,ttl(value) from %s.%s "+
			"where key = ?", gcpKeyspace, gcpBlobstore)
	}

	// ensure that reads see curTTL < ttl when ttl > 0
	// benchmark time isn't affected due to following ResetTimer
	if ttl > 0 {
		time.Sleep(1 * time.Second)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var value []byte
		var curTTL int
		var err error
		for pb.Next() {
			// the if check impacts all benchmarks so not a concern
			if ttl < 0 {
				err = db.Query(rQryStr, []byte(key)).Scan(&value)
			} else {
				err = db.Query(rQryStr, []byte(key)).Scan(&value, &curTTL)
			}
			if err != nil {
				b.Fatal(err)
				return
			}
			// basic quick check
			if !bytes.Equal(value[:len(check)], check) {
				b.Fatalf("Found: %v Expected %v",
					value[:len(check)], check)
				return
			}
			// This check is needed but it shouldn't
			//   add any overhead to skew the with-TTL and
			//   without-TTL comparisons
			if (ttl == 0 && curTTL != 0) ||
				(ttl > 0 && (curTTL == 0 || curTTL >= ttl)) {
				b.Fatalf("curTTL: %d ttl: %d", curTTL, ttl)
				return
			}
		}
	})
}

// use GoCQL APIs to benchmark blob writes
func benchBlobWrites(b *testing.B, cluster *gocql.ClusterConfig,
	key string, value []byte, ttl int) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	var qryStr string
	if ttl < 0 {
		qryStr = fmt.Sprintf("insert into %s.%s (key, value)"+
			"values (?,?)", gcpKeyspace, gcpBlobstore)
	} else {
		qryStr = fmt.Sprintf("insert into %s.%s (key, value)"+
			"values (?,?) using ttl %d", gcpKeyspace, gcpBlobstore, ttl)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query(qryStr, []byte(key), value).Exec()
			if err != nil {
				b.Fatal(err)
				return
			}
		}
	})
}

func benchBlob256KBWrites(b *testing.B, cluster *gocql.ClusterConfig, ttl int) {

	buf := make([]byte, 256*1024)
	benchBlobWrites(b, cluster, "benchBlob256KBWrites", buf, ttl)
}

func benchBlob256KBCachedReads(b *testing.B, cluster *gocql.ClusterConfig, ttl int) {

	buf := make([]byte, 256*1024)
	check := []byte{0xde, 0xad, 0xbe, 0xef}
	copy(buf, check)

	benchBlobCachedReads(b, cluster, "deadbeef", check, buf, ttl)
}

// benchmark to get latency information for a small write
func benchBlob512BWrites(b *testing.B, cluster *gocql.ClusterConfig, ttl int) {

	buf := make([]byte, 512)
	benchBlobWrites(b, cluster, "benchBlob512BWrites", buf, ttl)
}

// benchmark to get latency information for a small cached read
func benchBlob512BCachedReads(b *testing.B, cluster *gocql.ClusterConfig, ttl int) {

	buf := make([]byte, 512)
	check := []byte{0xde, 0xad, 0xbe, 0xef}
	copy(buf, check)

	benchBlobCachedReads(b, cluster, "deadbeef", check, check, ttl)
}

func BenchmarkGoCQL(b *testing.B) {
	confFile, err := EtherConfFile()
	if err != nil {
		b.Fatalf("Getting ether configuration failed: %s", err)
	}

	cfg, err1 := readCqlConfig(confFile)
	if err1 != nil {
		b.Fatalf("Reading ether config file failed: %s", err1)
	}
	gcpKeyspace = cfg.Cluster.KeySpace
	cluster := setupCluster(cfg.Cluster)

	// The benchmarks themselves aren't dependent
	// on each other but the benchmark report looks more readable
	// when the output is in order
	bmarks := []struct {
		name     string
		testFunc func(*testing.B, *gocql.ClusterConfig, int)
		ttl      int
	}{
		{"Wsdb-Writes", benchWsdbWrites, -1},
		{"Wsdb-CachedReads", benchWsdbCachedReads, -1},

		// large blob io: no TTL, TTL=0, TTL > 0
		{"Blob-256KB-Writes", benchBlob256KBWrites, -1},
		{"Blob-256KB-Writes-TTL0", benchBlob256KBWrites, 0},
		{"Blob-256KB-Writes-TTL", benchBlob256KBWrites, 3600},
		{"Blob-256KB-CachedReads", benchBlob256KBCachedReads, -1},
		{"Blob-256KB-CachedReads-TTL0", benchBlob256KBCachedReads, 0},
		{"Blob-256KB-CachedReads-TTL", benchBlob256KBCachedReads, 3600},

		// small blob io: no TTL, TTL=0, TTL > 0
		{"Blob-512B-Writes", benchBlob512BWrites, -1},
		{"Blob-512B-Writes-TTL0", benchBlob512BWrites, 0},
		{"Blob-512B-Writes-TTL", benchBlob512BWrites, 3600},
		{"Blob-512B-CachedReads", benchBlob512BCachedReads, -1},
		{"Blob-512B-CachedReads-TTL0", benchBlob512BCachedReads, 0},
		{"Blob-512B-CachedReads-TTL", benchBlob512BCachedReads, 3600},
	}

	// ensure a clean state
	err = DoTestSchemaOp(confFile, SchemaDelete)
	if err != nil {
		b.Fatalf("TearDownScheme failed error: %s", err)
	}
	// clean up upon any b.Run failures
	defer DoTestSchemaOp(confFile, SchemaDelete)

	for _, subBmark := range bmarks {
		// setup and teardown of schema for each kind of
		// benchmark to enable a clean state
		if err = DoTestSchemaOp(confFile, SchemaCreate); err != nil {
			b.Fatalf("DoTestSchemaOp SchemaCreate returned an error: %s", err)
		}

		b.Run(fmt.Sprintf("%s", subBmark.name),
			func(b *testing.B) {
				subBmark.testFunc(b, cluster, subBmark.ttl)
			})

		if err = DoTestSchemaOp(confFile, SchemaDelete); err != nil {
			b.Fatalf("TearDownSchema returned an error: %s", err)
		}
	}
}
