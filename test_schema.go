// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
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

// SetupTestSchema does the setup of schema for docker/k8s
//   CREATE KEYSPACE IF NOT EXISTS ether WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
//   CREATE TABLE IF NOT EXISTS ether.blobstore ( key text PRIMARY KEY, value blob );
//   CREATE TABLE IF NOT EXISTS ether.workspacedb ( namespace text, workspace text, key text, PRIMARY KEY ( namespace, workspace )) ;
func SetupTestSchema(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}
	session, err := getTestClusterSession(cfg.Cluster)
	if err != nil {
		return fmt.Errorf("error in creating gocql session inside SetupTestSchema: %s", err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = "+
		"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", cfg.Cluster.KeySpace)

	query := session.Query(queryStr)
	err = execWithRetry(query)

	if err != nil {
		return fmt.Errorf("error in creating keyspace %s: %s", cfg.Cluster.KeySpace, err.Error())
	}

	queryStr = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.blobstore"+
		"( key text PRIMARY KEY, value blob )", cfg.Cluster.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in creating Table  %s: %s", "blobstore", err.Error())
	}

	queryStr = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.workspacedb"+
		" ( namespace text, workspace text, key blob, PRIMARY KEY ( namespace, workspace ))",
		cfg.Cluster.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in creating Table  %s: %s", "workspacedb", err.Error())
	}

	return nil
}

// TearDownTestSchema drops the entire keyspace.
func TearDownTestSchema(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}
	session, err := getTestClusterSession(cfg.Cluster)
	if err != nil {
		return fmt.Errorf("error in creating gocql session inside TearDownTestSchema: %s",
			err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("DROP keyspace if exists %s", cfg.Cluster.KeySpace)
	query := session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in dropping keyspace %s: %s", cfg.Cluster.KeySpace, err.Error())
	}
	return nil
}

// TruncateTable truncates the blobstore and workspacedb tables in the
// keyspace. Not used right now. Can be used later.
func TruncateTable(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}
	session, err := getTestClusterSession(cfg.Cluster)
	if err != nil {
		return fmt.Errorf("error in creating gocql session inside TruncateTable: %s",
			err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("Truncate %s.blobstore", cfg.Cluster.KeySpace)
	query := session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in Truncate Table blobstore: %s", err.Error())
	}
	queryStr = fmt.Sprintf("Truncate %s.workspacedb", cfg.Cluster.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in Truncate Table workspacedb: %s", err.Error())
	}
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
