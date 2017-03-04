// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build gocql

package cql

import (
	"bytes"
	"fmt"
	"testing"

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

func setupCluster(cfg ClusterConfig) *gocql.ClusterConfig {
	c := NewRealCluster(cfg)
	rc, _ := c.(*RealCluster)

	return rc.cluster
}

// use GoCQL APIs to benchmark wsdb writes
func benchWsdbWrites(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	qryStr := fmt.Sprintf("insert into %s.workspacedb("+
		"typespace, namespace, workspace, key) values (?,?,?,?)",
		keyspace)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query(qryStr, "wsdb", "bench", "test", nil).Exec()
			if err != nil {
				b.Fatal(err)
				return
			}
		}
	})
}

// use GoCQL APIs to benchmark wsdb cached reads
func benchWsdbCachedReads(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	// setup the data to be read
	wQryStr := fmt.Sprintf("insert into %s.workspacedb("+
		"typespace, namespace, workspace, key) values (?,?,?,?)",
		keyspace)
	err = db.Query(wQryStr, "wsdb", "bench", "test", nil).Exec()
	if err != nil {
		b.Fatal(err)
		return
	}

	b.ResetTimer()
	b.ReportAllocs()

	// must use the index in the table else the query takes too long
	// since all the nodes are checked for data
	rQryStr := fmt.Sprintf("select typespace, namespace, workspace, key "+
		"from %s.workspacedb where typespace='wsdb' and namespace='bench' "+
		"and workspace='test'", keyspace)

	b.RunParallel(func(pb *testing.PB) {
		var typespace, workspace, namespace string
		var key []byte

		for pb.Next() {
			err := db.Query(rQryStr).Scan(&typespace, &namespace, &workspace, &key)
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
	key string, check []byte, writeBuf []byte,
	keyspace string) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	wQryStr := fmt.Sprintf("insert into %s.blobStore (key, value)"+
		"values (?,?)", keyspace)
	err = db.Query(wQryStr, key, writeBuf).Exec()
	if err != nil {
		b.Fatal(err)
		return
	}

	b.ResetTimer()
	b.ReportAllocs()

	// must use the index in the table else the query takes too long
	// since all the nodes are checked for data
	rQryStr := fmt.Sprintf("select value from %s.blobStore "+
		"where key='%s'", keyspace, key)

	b.RunParallel(func(pb *testing.PB) {
		var value []byte
		for pb.Next() {
			err := db.Query(rQryStr).Scan(&value)
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
		}
	})
}

// use GoCQL APIs to benchmark blob writes
func benchBlobWrites(b *testing.B, cluster *gocql.ClusterConfig,
	key string, value []byte, keyspace string) {

	db, err := cluster.CreateSession()
	if err != nil {
		b.Fatalf("Create session failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	qryStr := fmt.Sprintf("insert into %s.blobStore (key, value)"+
		"values (?,?)", keyspace)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Query(qryStr, key, value).Exec()
			if err != nil {
				b.Fatal(err)
				return
			}
		}
	})
}

func benchBlob1MBWrites(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	buf := make([]byte, 1024*1024)
	benchBlobWrites(b, cluster, "benchBlob1MBWrites", buf, keyspace)
}

func benchBlob1MBCachedReads(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	buf := make([]byte, 1024*1024)
	check := []byte{0xde, 0xad, 0xbe, 0xef}
	copy(buf, check)

	benchBlobCachedReads(b, cluster, "deadbeef", check, buf, keyspace)
}

// benchmark to get latency information for a small write
func benchBlob4BWrites(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	buf := []byte{0xde, 0xad, 0xbe, 0xef}
	benchBlobWrites(b, cluster, "benchBlob4BWrites", buf, keyspace)
}

// benchmark to get latency information for a small cached read
func benchBlob4BCachedReads(b *testing.B, cluster *gocql.ClusterConfig,
	keyspace string) {

	check := []byte{0xde, 0xad, 0xbe, 0xef}

	benchBlobCachedReads(b, cluster, "deadbeef", check, check, keyspace)
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
	cluster := setupCluster(cfg.Cluster)

	bmarks := map[string]func(*testing.B, *gocql.ClusterConfig, string){
		"Wsdb-Writes":          benchWsdbWrites,
		"Wsdb-CachedReads":     benchWsdbCachedReads,
		"Blob-1MB-Writes":      benchBlob1MBWrites,
		"Blob-1MB-CachedReads": benchBlob1MBCachedReads,
		"Blob-4B-Writes":       benchBlob4BWrites,
		"Blob-4B-CachedReads":  benchBlob4BCachedReads,
	}

	// ensure a clean state
	err = TearDownTestSchema(confFile)
	if err != nil {
		b.Fatalf("TearDownScheme failed error: %s", err)
	}
	// clean up upon any b.Run failures
	defer TearDownTestSchema(confFile)

	for subBmarkName, subBmarkFunc := range bmarks {
		// setup and teardown of schema for each kind of
		// benchmark to enable a clean state
		if err = SetupTestSchema(confFile); err != nil {
			b.Fatalf("SetupSchema returned an error: %s", err)
		}

		b.Run(fmt.Sprintf("%s", subBmarkName),
			func(b *testing.B) {
				subBmarkFunc(b, cluster, cfg.Cluster.KeySpace)
			})

		if err = TearDownTestSchema(confFile); err != nil {
			b.Fatalf("TearDownSchema returned an error: %s", err)
		}
	}
}
