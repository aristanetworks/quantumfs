// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// In this implementation of workspace DB API
// every API interacts with CQL datastore. There is
// no caching. Any caches must be layered on top of
// this implementation

import (
	"bytes"
	"fmt"

	"github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/gocql/gocql"
)

type noCacheWsdb struct {
	store    *cqlStore
	keyspace string
}

func newNoCacheWsdb(cluster Cluster, cfg *Config) (wsdb.WorkspaceDB, error) {
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

	err = wsdb.wsdbKeyPut("_null", "null", []byte(nil))
	if err != nil {
		return nil, err
	}

	return wsdb, nil
}

// --- workspace DB API implementation ---

func (nc *noCacheWsdb) NumNamespaces() (int, error) {
	count, _, err := nc.fetchDBNamespaces()
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumNamespaces: %s", err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) NamespaceList() ([]string, error) {
	_, list, err := nc.fetchDBNamespaces()
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceList: %s", err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) NumWorkspaces(namespace string) (int, error) {
	count, _, err := nc.fetchDBWorkspaces(namespace)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumWorkspaces %s : %s",
			namespace, err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) WorkspaceList(namespace string) ([]string, error) {
	_, list, err := nc.fetchDBWorkspaces(namespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceList %s : %s",
			namespace, err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) NamespaceExists(namespace string) (bool, error) {
	exists, err := nc.wsdbNamespaceExists(namespace)
	if err != nil {
		return exists, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceExists %s : %s", namespace, err.Error())
	}

	return exists, nil
}

func (nc *noCacheWsdb) WorkspaceExists(namespace string,
	workspace string) (bool, error) {

	_, present, err := nc.wsdbKeyGet(namespace, workspace)
	if err != nil {
		return present, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceExists %s/%s : %s",
			namespace, workspace, err.Error())
	}

	return present, nil
}

func (nc *noCacheWsdb) BranchWorkspace(srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	key, present, err := nc.wsdbKeyGet(srcNamespace, srcWorkspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s : %s ",
			srcNamespace, srcWorkspace,
			err.Error())
	}

	if !present {
		return wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot branch workspace: %s/%s", srcNamespace, srcWorkspace)
	}

	// branching to an existing workspace shouldn't be allowed
	_, present, err = nc.wsdbKeyGet(dstNamespace, dstWorkspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s : %s",
			dstNamespace, dstWorkspace,
			err.Error())
	}

	if present {
		return wsdb.NewError(wsdb.ErrWorkspaceExists,
			"cannot branch workspace: %s/%s", dstNamespace, dstWorkspace)
	}

	if err = nc.wsdbKeyPut(dstNamespace, dstWorkspace, key); err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in BranchWorkspace %s/%s : %s",
			dstNamespace, dstWorkspace,
			err.Error())
	}

	return nil
}

func (nc *noCacheWsdb) Workspace(namespace string,
	workspace string) (wsdb.ObjectKey, error) {

	key, present, err := nc.wsdbKeyGet(namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Get in Workspace %s/%s : %s",
			namespace, workspace, err.Error())
	}

	if !present {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"during Workspace %s/%s", namespace, workspace)
	}

	return key, nil
}

func (nc *noCacheWsdb) AdvanceWorkspace(namespace string,
	workspace string, currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, error) {

	key, present, err := nc.wsdbKeyGet(namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Get in AdvanceWorkspace %s/%s : %s",
			namespace, workspace, err.Error())
	}

	if !present {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot advance workspace %s/%s", namespace, workspace)
	}

	if !bytes.Equal(currentRootID, key) {
		return key, wsdb.NewError(wsdb.ErrWorkspaceOutOfDate,
			"cannot advance workspace %s/%s", currentRootID, key)
	}

	if err := nc.wsdbKeyPut(namespace, workspace, newRootID); err != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Put in AdvanceWorkspace %s/%s : %s",
			namespace, workspace, err.Error())
	}

	return newRootID, nil
}

// --- helper routines ---

func (nc *noCacheWsdb) wsdbNamespaceExists(namespace string) (bool, error) {
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
			return false, nil
		default:
			return false, err
		}
	} else {
		return true, nil
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

func (nc *noCacheWsdb) wsdbKeyGet(namespace string,
	workspace string) (key []byte, present bool, err error) {

	qryStr := fmt.Sprintf(`
SELECT key
FROM %s.workspacedb
WHERE namespace = ? AND workspace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, namespace, workspace)

	err = query.Scan(&key)
	if err != nil {
		switch err {
		case gocql.ErrNotFound:
			return nil, false, nil
		default:
			return nil, false, err
		}
	} else {
		return key, true, nil
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
