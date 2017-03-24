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

	wsdbInst := &noCacheWsdb{
		store:    &store,
		keyspace: cfg.Cluster.KeySpace,
	}

	err = wsdbInst.wsdbKeyPut(wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte(nil))
	if err != nil {
		return nil, err
	}

	return wsdbInst, nil
}

// --- workspace DB API implementation ---

func (nc *noCacheWsdb) NumTypespaces() (int, error) {
	count, _, err := nc.fetchDBTypespaces()
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumTypespaces: %s", err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) TypespaceList() ([]string, error) {
	_, list, err := nc.fetchDBTypespaces()
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during TypespaceList: %s", err.Error())
	}
	return list, nil
}
func (nc *noCacheWsdb) NumNamespaces(typespace string) (int, error) {
	count, _, err := nc.fetchDBNamespaces(typespace)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumNamespaces %s : %s", typespace,
			err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) NamespaceList(typespace string) ([]string, error) {
	_, list, err := nc.fetchDBNamespaces(typespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceList %s: %s", typespace,
			err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) NumWorkspaces(typespace string,
	namespace string) (int, error) {

	count, _, err := nc.fetchDBWorkspaces(typespace, namespace)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumWorkspaces %s/%s : %s",
			typespace, namespace, err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) WorkspaceList(typespace string,
	namespace string) ([]string, error) {

	_, list, err := nc.fetchDBWorkspaces(typespace, namespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceList %s/%s : %s",
			typespace, namespace, err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) TypespaceExists(typespace string) (bool, error) {
	exists, err := nc.wsdbTypespaceExists(typespace)
	if err != nil {
		return exists, wsdb.NewError(wsdb.ErrFatal,
			"during TypespaceExists %s : %s", typespace, err.Error())
	}

	return exists, nil
}

func (nc *noCacheWsdb) NamespaceExists(typespace string,
	namespace string) (bool, error) {

	exists, err := nc.wsdbNamespaceExists(typespace, namespace)
	if err != nil {
		return exists, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceExists %s/%s : %s", typespace,
			namespace, err.Error())
	}

	return exists, nil
}

func (nc *noCacheWsdb) WorkspaceExists(typespace string, namespace string,
	workspace string) (bool, error) {

	_, present, err := nc.wsdbKeyGet(typespace, namespace, workspace)
	if err != nil {
		return present, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceExists %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return present, nil
}

func isTypespaceReserved(typespace string) bool {
	// verify that this is not an attempt to alter the
	// seed/first workspaceDB entry setup by ether implementation of workspaceDB
	return typespace == wsdb.NullSpaceName
}

func (nc *noCacheWsdb) BranchWorkspace(srcTypespace string,
	srcNamespace string, srcWorkspace string,
	dstTypespace string, dstNamespace string, dstWorkspace string) error {

	if isTypespaceReserved(dstTypespace) {
		return wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	key, present, err := nc.wsdbKeyGet(srcTypespace, srcNamespace,
		srcWorkspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s/%s : %s ",
			srcTypespace, srcNamespace, srcWorkspace,
			err.Error())
	}

	if !present {
		return wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot branch workspace: %s/%s/%s",
			srcTypespace, srcNamespace, srcWorkspace)
	}

	// branching to an existing workspace shouldn't be allowed
	_, present, err = nc.wsdbKeyGet(dstTypespace, dstNamespace, dstWorkspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s/%s : %s",
			dstTypespace, dstNamespace, dstWorkspace,
			err.Error())
	}

	if present {
		return wsdb.NewError(wsdb.ErrWorkspaceExists,
			"cannot branch workspace: %s/%s/%s",
			dstTypespace, dstNamespace, dstWorkspace)
	}

	if err = nc.wsdbKeyPut(dstTypespace, dstNamespace,
		dstWorkspace, key); err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in BranchWorkspace %s/%s/%s : %s",
			dstTypespace, dstNamespace, dstWorkspace,
			err.Error())
	}

	return nil
}

func (nc *noCacheWsdb) Workspace(typespace string, namespace string,
	workspace string) (wsdb.ObjectKey, error) {

	key, present, err := nc.wsdbKeyGet(typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Get in Workspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	if !present {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"during Workspace %s/%s/%s", typespace, namespace, workspace)
	}

	return key, nil
}

func (nc *noCacheWsdb) AdvanceWorkspace(typespace string,
	namespace string, workspace string, currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, error) {

	if isTypespaceReserved(typespace) && currentRootID != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	key, present, err := nc.wsdbKeyGet(typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Get in AdvanceWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	if !present {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot advance workspace %s/%s/%s", typespace,
			namespace, workspace)
	}

	if !bytes.Equal(currentRootID, key) {
		return key, wsdb.NewError(wsdb.ErrWorkspaceOutOfDate,
			"cannot advance workspace expected:%s found:%s",
			currentRootID, key)
	}

	if err := nc.wsdbKeyPut(typespace, namespace, workspace,
		newRootID); err != nil {

		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Put in AdvanceWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return newRootID, nil
}

// --- helper routines ---

func (nc *noCacheWsdb) wsdbTypespaceExists(typespace string) (bool, error) {
	qryStr := fmt.Sprintf(`
SELECT typespace
FROM %s.workspacedb
WHERE typespace = ? LIMIT 1`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace)

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

func (nc *noCacheWsdb) fetchDBTypespaces() (int, []string, error) {
	qryStr := fmt.Sprintf(`
SELECT distinct typespace
FROM %s.workspacedb`, nc.keyspace)

	query := nc.store.session.Query(qryStr)
	iter := query.Iter()
	count := 0
	var tempTypespace string
	var typespaceList []string
	for iter.Scan(&tempTypespace) {
		typespaceList = append(typespaceList, tempTypespace)
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, typespaceList, nil
}

func (nc *noCacheWsdb) wsdbNamespaceExists(typespace string,
	namespace string) (bool, error) {

	qryStr := fmt.Sprintf(`
SELECT namespace
FROM %s.workspacedb
WHERE typespace = ? AND namespace = ? LIMIT 1`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace, namespace)

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

func (nc *noCacheWsdb) fetchDBNamespaces(
	typespace string) (int, []string, error) {

	qryStr := fmt.Sprintf(`
SELECT namespace
FROM %s.workspacedb
WHERE typespace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace)
	iter := query.Iter()
	count := 0
	var tempNamespace string
	var namespaceList []string
	found := make(map[string]bool)
	for iter.Scan(&tempNamespace) {
		if _, exists := found[tempNamespace]; !exists {
			namespaceList = append(namespaceList, tempNamespace)
			count++
			found[tempNamespace] = true
		}
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, namespaceList, nil
}

func (nc *noCacheWsdb) fetchDBWorkspaces(typespace string,
	namespace string) (int, []string, error) {

	qryStr := fmt.Sprintf(`
SELECT workspace
FROM %s.workspacedb
WHERE typespace = ? AND namespace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace)

	iter := query.Iter()
	count := 0
	var tempWorkspace string
	var workspaceList []string
	for iter.Scan(&tempWorkspace) {
		workspaceList = append(workspaceList, tempWorkspace)
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, workspaceList, nil
}

func (nc *noCacheWsdb) wsdbKeyGet(typespace string,
	namespace string,
	workspace string) (key []byte, present bool, err error) {

	qryStr := fmt.Sprintf(`
SELECT key
FROM %s.workspacedb
WHERE typespace = ? AND namespace = ? AND workspace = ?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

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

func (nc *noCacheWsdb) wsdbKeyPut(typespace string,
	namespace string, workspace string,
	key []byte) error {

	qryStr := fmt.Sprintf(`
INSERT INTO %s.workspacedb
(typespace, namespace, workspace, key)
VALUES (?,?,?,?)`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace, key)

	return query.Exec()
}
