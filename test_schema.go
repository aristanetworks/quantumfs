// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
)

var schemaRetries = 20
var schemaTimeout = 3 * time.Second

func execWithRetry(q *gocql.Query) error {
	var err error
	var i int
	for i = 0; i < schemaRetries; i++ {
		err = q.Exec()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		fmt.Printf("ETHER: Failed after %d attempts query: %q\n", i, q)
	} else {
		if i > 0 {
			fmt.Printf("ETHER: Took %d attempts query: %q\n", i+1, q)
		}
	}
	return err
}

type SchemaOp int

const (
	SCHEMA_CREATE SchemaOp = iota
	SCHEMA_DELETE
)

func (s SchemaOp) String() string {
	switch s {
	case SCHEMA_CREATE:
		return "create"
	case SCHEMA_DELETE:
		return "delete"
	}

	return fmt.Sprintf("unknown schema op(%d)", s)
}

func doTableOp(sess *gocql.Session, op SchemaOp,
	keyspace string, bsName string, wsdbName string) error {

	var ddls []string
	if op == SCHEMA_CREATE {
		ddls = []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s"+
				" ( key blob PRIMARY KEY, value blob )"+
				" WITH compaction = { 'class': 'LeveledCompactionStrategy' };",
				keyspace, bsName),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s"+
				" ( typespace text, namespace text,"+
				" workspace text, key blob, nonce bigint,"+
				" immutable boolean,"+
				" PRIMARY KEY ( typespace, namespace, workspace ))"+
				" WITH compaction = { 'class': 'LeveledCompactionStrategy' };",
				keyspace, wsdbName),
		}
	} else {
		ddls = []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", keyspace, bsName),
			fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", keyspace, wsdbName),
		}
	}

	for _, stmt := range ddls {
		query := sess.Query(stmt)
		err := execWithRetry(query)
		if err != nil {
			return fmt.Errorf("error %q during %s", err.Error(), stmt)
		}
	}

	return nil
}

// based on the standard limits on the keyspace and tablename
// we restrict the max length of prefix to 30 chars
const maxKsTblPrefixLen = 30

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

	c := NewRealCluster(cfg.Cluster)
	realc := c.(*RealCluster)
	sess, serr := realc.cluster.CreateSession()
	if serr != nil {
		return fmt.Errorf("error in creating session in DoTestSchemaOp: %s", serr.Error())
	}
	defer sess.Close()
	doTableOp(sess, op, cfg.Cluster.KeySpace, bsName, wsdbName)

	return nil
}

func getTestClusterConfig(clusterCfg ClusterConfig) *gocql.ClusterConfig {

	var cluster = gocql.NewCluster(clusterCfg.Nodes...)

	cluster.ProtoVersion = 3
	cluster.Consistency = gocql.Quorum
	cluster.PoolConfig.HostSelectionPolicy =
		gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Events.DisableSchemaEvents = true

	// Hardcoded for testing
	cluster.Timeout = schemaTimeout
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: schemaRetries}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: clusterCfg.Username,
		Password: clusterCfg.Password,
	}

	return cluster
}

func getTestClusterSession(clusterCfg ClusterConfig) (*gocql.Session, error) {

	cluster := getTestClusterConfig(clusterCfg)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error in creating session: %s", err.Error())
	}

	return session, nil
}
