// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
)

type workspaceDB struct {
	store    *cqlStore
	keyspace string
}

// NewWorkspaceDB returns a new quantumfs.WorkspaceDB
// Also ensures that an active session exists with the cql cluster
func NewWorkspaceDB(confName string) quantumfs.WorkspaceDB {

	cfg, err := readCqlConfig(confName)
	if err != nil {
		fmt.Println("Error reading CQL config: ", err)
		panic(err.Error())
	}

	cluster := NewRealCluster(cfg.Cluster)
	var store cqlStore
	store, err = initCqlStore(cluster)
	if err != nil {
		panic(err)
	}

	wsdb := &workspaceDB{
		store:    &store,
		keyspace: cfg.Cluster.KeySpace,
	}

	return wsdb
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx) int {
	return 0
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx) []string {
	return nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, namespace string) int {
	return 0
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, namespace string) []string {
	return nil
}

func (wsdb *workspaceDB) NamespaceExists(c *quantumfs.Ctx, namespace string) bool {
	return false
}

func (wsdb *workspaceDB) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) bool {
	return false
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {
	return nil
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) quantumfs.ObjectKey {
	// TBD: This API should have a way to pass back invalid key or error
	var empty []byte
	return quantumfs.NewObjectKeyFromBytes(empty)
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootID quantumfs.ObjectKey,
	newRootID quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	var empty []byte
	return quantumfs.NewObjectKeyFromBytes(empty), nil
}
