// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// In this implementation of workspace DB API
// every API interacts with CQL datastore. There is
// no caching. Any caches must be layered on top of
// this implementation

import (
	"errors"
	"fmt"
	"github.com/aristanetworks/quantumfs"
	"github.com/gocql/gocql"
)

type noCacheWsdb struct {
	store    *cqlStore
	keyspace string
}

func newNoCacheWsdb(cluster Cluster, cfg *Config) (quantumfs.WorkspaceDB, error) {
	var store cqlStore
	var err error

	store, err = initCqlStore(cluster)
	if err != nil {
		return nil, err
	}

	wsdb := &noCacheWsdb{
		store:    &store,
		keyspace: cfg.Cluster.KeySpace,
	}

	err = wsdb.wsdbKeyPut("_null", "null", quantumfs.EmptyWorkspaceKey.Value())
	if err != nil {
		return nil, err
	}

	return wsdb, nil
}

// --- workspace DB API implementation ---

func (nc *noCacheWsdb) NumNamespaces(c *quantumfs.Ctx) int {
	count, _, err := nc.fetchDBNamespaces()
	if err != nil {
		return 0
	}
	return count
}

func (nc *noCacheWsdb) NamespaceList(c *quantumfs.Ctx) []string {
	_, list, err := nc.fetchDBNamespaces()
	if err != nil {
		return nil
	}
	return list
}

func (nc *noCacheWsdb) NumWorkspaces(c *quantumfs.Ctx, namespace string) int {
	count, _, err := nc.fetchDBWorkspaces(namespace)
	if err != nil {
		return 0
	}
	return count
}

func (nc *noCacheWsdb) WorkspaceList(c *quantumfs.Ctx, namespace string) []string {
	_, list, err := nc.fetchDBWorkspaces(namespace)
	if err != nil {
		return nil
	}
	return list
}

func (nc *noCacheWsdb) NamespaceExists(c *quantumfs.Ctx, namespace string) bool {
	return nc.wsdbNamespaceExists(namespace)
}

func (nc *noCacheWsdb) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) bool {

	_, present := nc.wsdbKeyGet(namespace, workspace)
	return present
}

func (nc *noCacheWsdb) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	key, present := nc.wsdbKeyGet(srcNamespace, srcWorkspace)

	if !present {
		return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND)
	}

	// branching to an existing workspace shouldn't be allowed
	// TODO: define an explicit error WSDB_WORKSPACE_EXISTS
	_, present = nc.wsdbKeyGet(dstNamespace, dstWorkspace)
	if present {
		return errors.New("Workspace already exists")
	}

	return nc.wsdbKeyPut(dstNamespace, dstWorkspace, key)
}

func (nc *noCacheWsdb) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) quantumfs.ObjectKey {

	key, present := nc.wsdbKeyGet(namespace, workspace)

	if !present {
		panic("workspace not found in Workspace()")
	}

	objKey := quantumfs.NewObjectKeyFromBytes(key)
	return objKey
}

func (nc *noCacheWsdb) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootID quantumfs.ObjectKey,
	newRootID quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	key, present := nc.wsdbKeyGet(namespace, workspace)
	if !present {
		return quantumfs.ObjectKey{}, quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_WORKSPACE_NOT_FOUND)
	}

	objKey := quantumfs.NewObjectKeyFromBytes(key)

	if !currentRootID.IsEqualTo(objKey) {
		return quantumfs.ObjectKey{}, quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE)
	}

	if err := nc.wsdbKeyPut(namespace, workspace, newRootID.Value()); err != nil {
		panic(fmt.Sprintf("Panic advancing rootID: %v", err))
	}

	return newRootID, nil
}

// --- helper routines ---

func (nc *noCacheWsdb) wsdbNamespaceExists(namespace string) bool {
	qryStr := fmt.Sprintf(`
SELECT namespace
FROM %s.workspacedb
WHERE namespace = ? LIMIT 1`, nc.keyspace)

	query := nc.store.session.Query(qryStr, namespace)

	nspace := ""
	err := query.Scan(&nspace)
	if err != nil {
		switch err {
		case gocql.ErrNotFound:
			return false
		default:
			// TODO: handle error gracefully but how?
			panic(fmt.Sprintf("Error %q in NamespaceExists ", err))
		}
	} else {
		return true
	}

}

func (nc *noCacheWsdb) fetchDBNamespaces(keys ...string) (int, []string, error) {
	qryStr := fmt.Sprintf(`
SELECT distinct namespace
FROM %s.workspacedb`, nc.keyspace)

	query := nc.store.session.Query(qryStr)
	iter := query.Iter()
	count := 0
	var tempNamespace string
	namespaceList := make([]string, 0)
	for iter.Scan(&tempNamespace) {
		namespaceList = append(namespaceList, tempNamespace)
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, namespaceList, nil
}

func (nc *noCacheWsdb) fetchDBWorkspaces(keys ...string) (int, []string, error) {
	qryStr := fmt.Sprintf(`
SELECT workspace
FROM %s.workspacedb
WHERE namespace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, keys[0])

	iter := query.Iter()
	count := 0
	var tempWorkspace string
	workspaceList := make([]string, 0)
	for iter.Scan(&tempWorkspace) {
		workspaceList = append(workspaceList, tempWorkspace)
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, workspaceList, nil
}

func (nc *noCacheWsdb) wsdbKeyGet(namespace string, workspace string) (key []byte, present bool) {
	qryStr := fmt.Sprintf(`
SELECT key
FROM %s.workspacedb
WHERE namespace = ? AND workspace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, namespace, workspace)

	err := query.Scan(&key)
	if err != nil {
		switch err {
		case gocql.ErrNotFound:
			return nil, false
		default:
			panic(fmt.Sprintf("Error %q during wsdbKeyGet", err))
		}
	} else {
		return key, true
	}
}

func (nc *noCacheWsdb) wsdbKeyPut(namespace string, workspace string,
	key []byte) error {

	qryStr := fmt.Sprintf(`
INSERT INTO %s.workspacedb
(namespace, workspace, key)
VALUES (?,?,?)`, nc.keyspace)

	query := nc.store.session.Query(qryStr, namespace, workspace, key)

	return query.Exec()
}
