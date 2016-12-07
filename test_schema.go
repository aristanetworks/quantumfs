// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"time"
)

var numRetries = 20

func execWithRetry(q Query) error {
	var err error
	var i int
	for i = 0; i < numRetries; i++ {
		if err = q.Exec(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		fmt.Printf("Failed after %d attempts query: %q\n", i+1, q)
	} else {
		fmt.Printf("Took %d attempts query: %q\n", i+1, q)
	}
	return err
}

// SetupTestSchema does the setup of schema for docker/k8s
// CREATE KEYSPACE IF NOT EXISTS ether WITH REPLICATION = { 'class' : 'SimpleStrategy',
//   'replication_factor' : 1 };
// CREATE TABLE IF NOT EXISTS ether.blobstore ( key text PRIMARY KEY, value blob );
// CREATE TABLE IF NOT EXISTS ether.workspacedb ( namespace text, workspace text, key text,
//   PRIMARY KEY ( namespace, workspace )) ;
func SetupTestSchema(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}

	cluster := NewRealCluster(cfg.Nodes...)
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("error in creating session: %s", err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = "+
		"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", cfg.KeySpace)
	query := session.Query(queryStr)
	err = execWithRetry(query)

	if err != nil {
		return fmt.Errorf("error in creating keyspace %s: %s", cfg.KeySpace, err.Error())
	}

	queryStr = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.blobstore"+
		"( key text PRIMARY KEY, value blob )", cfg.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in creating Table  %s: %s", "blobstore", err.Error())
	}

	queryStr = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.workspacedb"+
		" ( namespace text, workspace text, key blob, PRIMARY KEY ( namespace, workspace ))",
		cfg.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in creating Table  %s: %s", "workspacedb", err.Error())
	}

	return nil
}

// TearDownTestSchema drops the c.keyspace keyspace
func TearDownTestSchema(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err.Error())
	}

	cluster := NewRealCluster(cfg.Nodes...)
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("error in creating session: %s", err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("DROP keyspace if exists %s", cfg.KeySpace)
	query := session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in dropping keyspace %s: %s", cfg.KeySpace, err.Error())
	}
	return nil
}

// TruncateTable truncates the blobstore and workspacedb tables in cfg.KeySpace keyspace
// Not used right now. Can be used later.
func TruncateTable(confFile string) error {

	cfg, err := readCqlConfig(confFile)
	if err != nil {
		return fmt.Errorf("error in reading cqlConfigFile: %s", err)
	}

	cluster := NewRealCluster(cfg.Nodes...)
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("error in creating session: %s", err.Error())
	}
	defer session.Close()

	queryStr := fmt.Sprintf("Truncate %s.blobstore", cfg.KeySpace)
	query := session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in Truncate Table blobstore: %s", err.Error())
	}
	queryStr = fmt.Sprintf("Truncate %s.workspacedb", cfg.KeySpace)
	query = session.Query(queryStr)
	err = execWithRetry(query)
	if err != nil {
		return fmt.Errorf("error in Truncate Table workspacedb: %s", err.Error())
	}
	return nil
}
