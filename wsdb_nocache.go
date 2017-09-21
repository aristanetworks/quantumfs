// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// In this implementation of workspace DB API
// every API interacts with CQL datastore. There is
// no caching. Any caches must be layered on top of
// this implementation

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/aristanetworks/ether"
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

	return wsdbInst, nil
}

// --- workspace DB API implementation ---

func (nc *noCacheWsdb) NumTypespaces(c ether.Ctx) (int, error) {
	defer c.FuncInName("noCacheWsdb::NumTypespaces").Out()

	count, _, err := nc.fetchDBTypespaces(c)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumTypespaces: %s", err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) TypespaceList(c ether.Ctx) ([]string, error) {
	defer c.FuncInName("noCacheWsdb::TypespaceList").Out()

	_, list, err := nc.fetchDBTypespaces(c)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during TypespaceList: %s", err.Error())
	}
	return list, nil
}
func (nc *noCacheWsdb) NumNamespaces(c ether.Ctx, typespace string) (int, error) {
	defer c.FuncIn("noCacheWsdb::NumNamespaces", "%s", typespace).Out()

	count, _, err := nc.fetchDBNamespaces(c, typespace)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumNamespaces %s : %s", typespace,
			err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) NamespaceList(c ether.Ctx, typespace string) ([]string, error) {
	defer c.FuncIn("noCacheWsdb::NamespaceList", "%s", typespace).Out()

	_, list, err := nc.fetchDBNamespaces(c, typespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceList %s: %s", typespace,
			err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) NumWorkspaces(c ether.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncIn("noCacheWsdb::NumWorkspaces", "%s/%s", typespace, namespace).Out()

	count, _, err := nc.fetchDBWorkspaces(c, typespace, namespace)
	if err != nil {
		return 0, wsdb.NewError(wsdb.ErrFatal,
			"during NumWorkspaces %s/%s : %s",
			typespace, namespace, err.Error())
	}
	return count, nil
}

func (nc *noCacheWsdb) WorkspaceList(c ether.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncIn("noCacheWsdb::WorkspaceList", "%s/%s", typespace, namespace).Out()

	_, list, err := nc.fetchDBWorkspaces(c, typespace, namespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceList %s/%s : %s",
			typespace, namespace, err.Error())
	}
	return list, nil
}

func (nc *noCacheWsdb) TypespaceExists(c ether.Ctx, typespace string) (bool, error) {
	defer c.FuncIn("noCacheWsdb::TypespaceExists", "%s", typespace).Out()

	exists, err := nc.wsdbTypespaceExists(c, typespace)
	if err != nil {
		return exists, wsdb.NewError(wsdb.ErrFatal,
			"during TypespaceExists %s : %s", typespace, err.Error())
	}

	return exists, nil
}

func (nc *noCacheWsdb) NamespaceExists(c ether.Ctx, typespace string,
	namespace string) (bool, error) {

	defer c.FuncIn("noCacheWsdb::NamespaceExists", "%s/%s", typespace, namespace).Out()

	exists, err := nc.wsdbNamespaceExists(c, typespace, namespace)
	if err != nil {
		return exists, wsdb.NewError(wsdb.ErrFatal,
			"during NamespaceExists %s/%s : %s", typespace,
			namespace, err.Error())
	}

	return exists, nil
}

func (nc *noCacheWsdb) WorkspaceExists(c ether.Ctx, typespace string, namespace string,
	workspace string) (bool, error) {

	defer c.FuncIn("noCacheWsdb::WorkspaceExists", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	_, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
	if err != nil {
		return present, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceExists %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return present, nil
}

// All workspaces under the wsdb.NullSpaceName
// typespace are locked. They cannot be deleted,
// cannot be advanced or cannot be destination
// of branch operation
func isTypespaceLocked(typespace string) bool {
	return typespace == wsdb.NullSpaceName
}

func (nc *noCacheWsdb) CreateWorkspace(c ether.Ctx, typespace string, namespace string,
	workspace string, wsKey wsdb.ObjectKey) error {

	keyHex := hex.EncodeToString(wsKey)
	defer c.FuncIn("noCacheWsdb::CreateWorkspace", "%s/%s/%s(%s)", typespace, namespace,
		workspace, keyHex).Out()

	err := nc.wsdbKeyPut(ether.DefaultCtx, typespace, namespace, workspace, wsKey)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in CreateWorkspace %s/%s/%s(%s) : %s",
			typespace, namespace, workspace, keyHex, err.Error())
	}

	return nil
}

func (nc *noCacheWsdb) BranchWorkspace(c ether.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string,
	dstTypespace string, dstNamespace string, dstWorkspace string) error {

	defer c.FuncIn("noCacheWsdb::BranchWorkspace", "%s/%s/%s -> %s/%s/%s)", srcTypespace,
		srcNamespace, srcWorkspace, dstTypespace, dstNamespace, dstWorkspace).Out()

	if isTypespaceLocked(dstTypespace) {
		return wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	key, present, err := nc.wsdbKeyGet(c, srcTypespace, srcNamespace,
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
	_, present, err = nc.wsdbKeyGet(c, dstTypespace, dstNamespace, dstWorkspace)
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

	if err = nc.wsdbKeyPut(c, dstTypespace, dstNamespace,
		dstWorkspace, key); err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in BranchWorkspace %s/%s/%s : %s",
			dstTypespace, dstNamespace, dstWorkspace,
			err.Error())
	}

	return nil
}

func (nc *noCacheWsdb) Workspace(c ether.Ctx, typespace string, namespace string,
	workspace string) (wsdb.ObjectKey, error) {

	defer c.FuncIn("noCacheWsdb::Workspace", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	key, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
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

func (nc *noCacheWsdb) DeleteWorkspace(c ether.Ctx, typespace string, namespace string,
	workspace string) error {

	defer c.FuncIn("noCacheWsdb::DeleteWorkspace", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	if isTypespaceLocked(typespace) {
		return wsdb.NewError(wsdb.ErrLocked,
			"Delete failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	err := nc.wsdbKeyDel(c, typespace, namespace, workspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Del in DeleteWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return nil
}

func (nc *noCacheWsdb) AdvanceWorkspace(c ether.Ctx, typespace string,
	namespace string, workspace string, currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, error) {

	currentKeyHex := hex.EncodeToString(currentRootID)
	newKeyHex := hex.EncodeToString(newRootID)

	defer c.FuncIn("noCacheWsdb::AdvanceWorkspace", "%s/%s/%s(%s -> %s)", typespace,
		namespace, workspace, currentKeyHex, newKeyHex).Out()

	if isTypespaceLocked(typespace) && currentRootID != nil {
		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	key, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
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
			currentKeyHex, hex.EncodeToString(key))
	}

	if err := nc.wsdbKeyPut(c, typespace, namespace, workspace,
		newRootID); err != nil {

		return wsdb.ObjectKey{}, wsdb.NewError(wsdb.ErrFatal,
			"during Put in AdvanceWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return newRootID, nil
}

func (nc *noCacheWsdb) WorkspaceLastWriteTime(c ether.Ctx, typespace string,
	namespace string, workspace string) (time.Time, error) {

	defer c.FuncIn("noCacheWsdb::WorkspaceLastWriteTime", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	microSec, err := nc.wsdbKeyLastWriteTime(c, typespace, namespace, workspace)
	if err != nil {
		return time.Time{}, wsdb.NewError(wsdb.ErrFatal,
			"during getting WorkspaceLastWriteTime %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	// CQL's write time is time in micro-second from epoch. Below we
	// convert it to golang's time.Time.
	ts := time.Unix(microSec/int64(time.Second/time.Microsecond), 0).UTC()

	return ts, nil
}

// --- helper routines ---

func (nc *noCacheWsdb) wsdbTypespaceExists(c ether.Ctx, typespace string) (bool, error) {
	defer c.FuncIn("noCacheWsdb::wsdbTypespaceExists", "%s", typespace).Out()

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

func (nc *noCacheWsdb) fetchDBTypespaces(c ether.Ctx) (int, []string, error) {
	defer c.FuncInName("noCacheWsdb::fetchDBTypespaces").Out()

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

func (nc *noCacheWsdb) wsdbNamespaceExists(c ether.Ctx, typespace string,
	namespace string) (bool, error) {

	defer c.FuncIn("noCacheWsdb::wsdbNamespaceExists", "%s/%s", typespace,
		namespace).Out()

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

func (nc *noCacheWsdb) fetchDBNamespaces(c ether.Ctx,
	typespace string) (int, []string, error) {

	defer c.FuncIn("noCacheWsdb::fetchDBNamespaces", "%s", typespace).Out()

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

func (nc *noCacheWsdb) fetchDBWorkspaces(c ether.Ctx, typespace string,
	namespace string) (int, []string, error) {

	defer c.FuncIn("noCacheWsdb::fetchDBWorkspaces", "%s/%s", typespace,
		namespace).Out()

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

func (nc *noCacheWsdb) wsdbKeyGet(c ether.Ctx, typespace string,
	namespace string, workspace string) (key []byte, present bool,
	err error) {

	defer c.FuncIn("noCacheWsdb::wsdbKeyGet", "%s/%s/%s", typespace,
		namespace, workspace).Out()

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

func (nc *noCacheWsdb) wsdbKeyDel(c ether.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn("noCacheWsdb::wsdbKeyDel", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
DELETE
FROM %s.workspacedb
WHERE typespace=? AND namespace=? AND workspace=?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	return query.Exec()
}

func (nc *noCacheWsdb) wsdbKeyPut(c ether.Ctx, typespace string,
	namespace string, workspace string,
	key []byte) error {

	defer c.FuncIn("noCacheWsdb::wsdbKeyPut", "%s/%s/%s key: %s", typespace,
		namespace, workspace, hex.EncodeToString(key)).Out()

	qryStr := fmt.Sprintf(`
INSERT INTO %s.workspacedb
(typespace, namespace, workspace, key)
VALUES (?,?,?,?)`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace, key)

	return query.Exec()
}

func (nc *noCacheWsdb) wsdbKeyLastWriteTime(c ether.Ctx, typespace string,
	namespace string, workspace string) (int64, error) {

	defer c.FuncIn("noCacheWsdb::wsdbKeyLastWriteTime", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
SELECT WRITETIME(key)
FROM %s.workspacedb
WHERE typespace=? AND namespace=? AND workspace=?`, nc.keyspace)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	var writeTime int64
	if err := query.Scan(&writeTime); err != nil {
		return int64(0), err
	}
	return writeTime, nil
}
