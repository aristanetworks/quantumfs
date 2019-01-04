// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
)

// SchemaOp is a type for Schema operations
type SchemaOp int

// Constants to define Schema operations
const (
	SchemaCreate SchemaOp = iota
	SchemaDelete
)

func (s SchemaOp) String() string {
	switch s {
	case SchemaCreate:
		return "create"
	case SchemaDelete:
		return "delete"
	}

	return fmt.Sprintf("unknown schema op(%d)", s)
}

var schemaRetries = 20

// DoTableOp is used to CREATE or DROP tables
func DoTableOp(sess *gocql.Session, op SchemaOp,
	keyspace string, bsName string, wsdbName string) error {

	var ddls []string
	if op == SchemaCreate {
		ddls = []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s"+
				" ( key blob PRIMARY KEY, value blob )"+
				" WITH compaction = "+
				" { 'class': 'LeveledCompactionStrategy' };",
				keyspace, bsName),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s"+
				" ( typespace text, namespace text,"+
				" workspace text, key blob, nonce bigint,"+
				" publishtime bigint, immutable boolean,"+
				" PRIMARY KEY ( typespace, namespace, workspace ))"+
				" WITH compaction = "+
				"{ 'class': 'LeveledCompactionStrategy' };",
				wsdbKeySpace(keyspace), wsdbName),
		}
	} else {
		ddls = []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
				keyspace, bsName),
			fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
				wsdbKeySpace(keyspace), wsdbName),
		}
	}

	for _, stmt := range ddls {
		query := sess.Query(stmt)
		err := execWithRetry(query, schemaRetries)
		if err != nil {
			return fmt.Errorf("error %q during %s", err.Error(), stmt)
		}
	}

	return nil
}

// based on the standard limits on the keyspace and tablename
// we restrict the max length of prefix to 30 chars
const maxKsTblPrefixLen = 30

// DoTestSchemaOp creates the test Schema
func DoTestSchemaOp(confFile string, op SchemaOp) error {
	prefix := os.Getenv("CFNAME_PREFIX")
	perr := checkCfNamePrefix(prefix)
	if perr != nil {
		return perr
	}

	bsName, wsdbName := prefixToTblNames(prefix)
	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}

	c := NewRealCluster(&cfg.Cluster)
	realc := c.(*RealCluster)
	sess, serr := realc.cluster.CreateSession()
	if serr != nil {
		return fmt.Errorf("error in creating session in DoTestSchemaOp: %s",
			serr.Error())
	}
	defer sess.Close()

	DoTableOp(sess, op, cfg.Cluster.KeySpace, bsName, wsdbName)

	return nil
}

// SetupIntegTestKeyspace is used to create the keyspace
// for integration tests. They keyspaces in hardware
// cluster should be setup by the admin
func SetupIntegTestKeyspace(confFile string) error {
	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}

	c := NewRealCluster(&cfg.Cluster)
	realc := c.(*RealCluster)
	sess, serr := realc.cluster.CreateSession()
	if serr != nil {
		return fmt.Errorf("error in creating session in DoTestSchemaOp: %s",
			serr.Error())
	}
	defer sess.Close()

	queryStr :=
		fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = "+
			"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
			cfg.Cluster.KeySpace)

	query := sess.Query(queryStr)
	err = execWithRetry(query, schemaRetries)

	if err != nil {
		return fmt.Errorf("error in creating keyspace %s: %s",
			cfg.Cluster.KeySpace, err.Error())
	}

	// workspacedb keyspace
	keyspace := wsdbKeySpace(cfg.Cluster.KeySpace)
	queryStr =
		fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = "+
			"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
			keyspace)

	query = sess.Query(queryStr)
	err = execWithRetry(query, schemaRetries)

	if err != nil {
		return fmt.Errorf("error in creating keyspace %s: %s",
			keyspace, err.Error())
	}

	return nil
}

func execWithRetry(q *gocql.Query, retries int) error {
	var err error
	var i int
	for i = 0; i < retries; i++ {
		err = q.Exec()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		fmt.Printf("CQL: Failed after %d attempts query: %q\n", i, q)
	} else {
		if i > 0 {
			fmt.Printf("CQL: Took %d attempts query: %q\n", i+1, q)
		}
	}
	return err
}
