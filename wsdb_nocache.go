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
	"os"
	"time"

	"github.com/aristanetworks/ether"
	"github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/gocql/gocql"
)

type noCacheWsdb struct {
	store    *cqlStore
	keyspace string
	cfName   string
}

func newNoCacheWsdb(cluster Cluster, cfg *Config) (wsdb.WorkspaceDB, error) {
	var store cqlStore
	var err error

	store, err = initCqlStore(cluster)
	if err != nil {
		return nil, err
	}

	_, wsdbName := prefixToTblNames(os.Getenv("CFNAME_PREFIX"))
	if err := isTablePresent(&store, cfg, wsdbName); err != nil {
		return nil, wsdb.NewError(wsdb.ErrFatal, "%s", err.Error())
	}

	wsdbInst := &noCacheWsdb{
		store:    &store,
		keyspace: cfg.Cluster.KeySpace,
		cfName:   wsdbName,
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
	namespace string) (map[string]wsdb.WorkspaceNonce, error) {

	defer c.FuncIn("noCacheWsdb::WorkspaceList", "%s/%s", typespace, namespace).Out()

	_, list, err := nc.fetchDBWorkspaces(c, typespace, namespace)
	if err != nil {
		return list, wsdb.NewError(wsdb.ErrFatal,
			"during WorkspaceList %s/%s : %s",
			typespace, namespace, err.Error())
	}
	return list, nil
}

// All workspaces under the wsdb.NullSpaceName
// typespace are locked. They cannot be deleted,
// cannot be advanced or cannot be destination
// of branch operation
func isTypespaceLocked(typespace string) bool {
	return typespace == wsdb.NullSpaceName
}

// CreateWorkspace is exclusively used in the ether adapter to create a _/_/_ workspace.
func (nc *noCacheWsdb) CreateWorkspace(c ether.Ctx, typespace string, namespace string,
	workspace string, nonce wsdb.WorkspaceNonce, wsKey wsdb.ObjectKey) error {

	keyHex := hex.EncodeToString(wsKey)
	defer c.FuncIn("noCacheWsdb::CreateWorkspace", "%s/%s/%s(%s)(%d)", typespace, namespace,
		workspace, keyHex, nonce).Out()

	// if the typespace/namespace/workspace already exists with a key
	// different than wsKey then raise error.
	// We do not care if _/_/_ was overwritten, or deleted-and-recreated as long
	// as it has the same key. Hence, ignoring the nonce here.
	existKey, _, present, _ := nc.wsdbKeyGet(c, typespace, namespace, workspace)
	if present && !bytes.Equal([]byte(wsKey), existKey) {
		existKeyHex := hex.EncodeToString(existKey)
		return wsdb.NewError(wsdb.ErrWorkspaceExists,
			"Cannot CreateWorkspace since different key exists for %s/%s/%s "+
				"want: %s found: %s", typespace, namespace, workspace,
			keyHex, existKeyHex)

	}

	err := nc.wsdbKeyPut(c, typespace, namespace, workspace, wsKey, nonce)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in CreateWorkspace %s/%s/%s(%s) : %s",
			typespace, namespace, workspace, keyHex, err.Error())
	}

	return nil
}

// Add new Nonce here.
func (nc *noCacheWsdb) BranchWorkspace(c ether.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string,
	dstTypespace string, dstNamespace string, dstWorkspace string) (wsdb.WorkspaceNonce, wsdb.WorkspaceNonce, error) {

	defer c.FuncIn("noCacheWsdb::BranchWorkspace", "%s/%s/%s -> %s/%s/%s)", srcTypespace,
		srcNamespace, srcWorkspace, dstTypespace, dstNamespace, dstWorkspace).Out()

	if isTypespaceLocked(dstTypespace) {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}
	key, srcNonce, present, err := nc.wsdbKeyGet(c, srcTypespace, srcNamespace,
		srcWorkspace)
	if err != nil {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s/%s : %s ",
			srcTypespace, srcNamespace, srcWorkspace,
			err.Error())
	}

	if !present {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot branch workspace: %s/%s/%s",
			srcTypespace, srcNamespace, srcWorkspace)
	}

	// branching to an existing workspace shouldn't be allowed
	_, _, present, err = nc.wsdbKeyGet(c, dstTypespace, dstNamespace, dstWorkspace)
	if err != nil {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Get in BranchWorkspace %s/%s/%s : %s",
			dstTypespace, dstNamespace, dstWorkspace,
			err.Error())
	}

	if present {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrWorkspaceExists,
			"cannot branch workspace: %s/%s/%s",
			dstTypespace, dstNamespace, dstWorkspace)
	}

	dstNonce := GetUniqueNonce()
	c.Vlog("Create Workspace %s/%s/%s dstNonce:%d",
		dstTypespace, dstNamespace, dstWorkspace, dstNonce)
	if err = nc.wsdbKeyPut(c, dstTypespace, dstNamespace,
		dstWorkspace, key, dstNonce); err != nil {
		return wsdb.WorkspaceNonceInvalid, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Put in BranchWorkspace %s/%s/%s dstNonce:(%d): %s",
			dstTypespace, dstNamespace, dstWorkspace, dstNonce,
			err.Error())
	}

	return srcNonce, dstNonce, nil
}

func (nc *noCacheWsdb) Workspace(c ether.Ctx, typespace string, namespace string,
	workspace string) (wsdb.ObjectKey, wsdb.WorkspaceNonce, error) {

	defer c.FuncIn("noCacheWsdb::Workspace", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	key, nonce, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Get in Workspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	if !present {
		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"during Workspace %s/%s/%s", typespace, namespace, workspace)
	}

	return key, nonce, nil
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
	namespace string, workspace string, currentNonce wsdb.WorkspaceNonce,
	currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, wsdb.WorkspaceNonce, error) {

	currentKeyHex := hex.EncodeToString(currentRootID)
	newKeyHex := hex.EncodeToString(newRootID)

	defer c.FuncIn("noCacheWsdb::AdvanceWorkspace", "%s/%s/%s(%s -> %s)", typespace,
		namespace, workspace, currentKeyHex, newKeyHex).Out()

	if isTypespaceLocked(typespace) && currentRootID != nil {
		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrLocked,
			"Branch failed: "+wsdb.NullSpaceName+" typespace is locked")
	}

	key, nonce, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Get in AdvanceWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	if !nonce.SameIncarnation(&currentNonce) {
		return key, nonce, wsdb.NewError(wsdb.ErrWorkspaceOutOfDate,
			"nonce mispatch Expected:%s Received:%s",
			currentNonce.String(), nonce.String())
	}
	if !present {
		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"cannot advance workspace %s/%s/%s", typespace,
			namespace, workspace)
	}

	if !bytes.Equal(currentRootID, key) {
		return key, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrWorkspaceOutOfDate,
			"cannot advance workspace expected:%s found:%s",
			currentKeyHex, hex.EncodeToString(key))
	}

	if err := nc.wsdbKeyPut(c, typespace, namespace, workspace,
		newRootID, currentNonce); err != nil {

		return wsdb.ObjectKey{}, wsdb.WorkspaceNonceInvalid, wsdb.NewError(wsdb.ErrFatal,
			"during Put in AdvanceWorkspace %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}

	return newRootID, currentNonce, nil
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

func (nc *noCacheWsdb) SetWorkspaceImmutable(c ether.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn("noCacheWsdb::SetWorkspaceImmutable", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	_, _, present, err := nc.wsdbKeyGet(c, typespace, namespace, workspace)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Get in SetWorkspaceImmutable %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}
	if !present {
		return wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"in SetWorkspaceImmutable workspace: %s/%s/%s",
			typespace, namespace, workspace)
	}

	err = nc.wsdbImmutablePut(c, typespace, namespace, workspace, true)
	if err != nil {
		return wsdb.NewError(wsdb.ErrFatal,
			"during Put in SetWorkspaceImmutable %s/%s/%s : %s",
			typespace, namespace, workspace, err.Error())
	}
	return nil
}

func (nc *noCacheWsdb) WorkspaceIsImmutable(c ether.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncIn("noCacheWsdb::WorkspaceIsImmutable", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	immutable, present, err := nc.wsdbImmutableGet(c, typespace, namespace, workspace)
	if err != nil {
		return false, wsdb.NewError(wsdb.ErrFatal,
			"during Get in WorkspaceIsImmutable %s/%s/%s(%s) : %s",
			typespace, namespace, workspace, err.Error())
	}
	if !present {
		return false, wsdb.NewError(wsdb.ErrWorkspaceNotFound,
			"in WorkspaceIsImmutable workspace: %s/%s/%s",
			typespace, namespace, workspace)
	}
	return immutable, nil
}

// --- helper routines ---

func (nc *noCacheWsdb) wsdbTypespaceExists(c ether.Ctx, typespace string) (bool, error) {
	defer c.FuncIn("noCacheWsdb::wsdbTypespaceExists", "%s", typespace).Out()

	qryStr := fmt.Sprintf(`
SELECT typespace
FROM %s.%s
WHERE typespace = ? LIMIT 1`, nc.keyspace, nc.cfName)

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
FROM %s.%s`, nc.keyspace, nc.cfName)

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
FROM %s.%s
WHERE typespace = ? AND namespace = ? LIMIT 1`, nc.keyspace, nc.cfName)

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
FROM %s.%s
WHERE typespace = ?`, nc.keyspace, nc.cfName)

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
	namespace string) (int, map[string]wsdb.WorkspaceNonce, error) {

	defer c.FuncIn("noCacheWsdb::fetchDBWorkspaces", "%s/%s", typespace,
		namespace).Out()

	qryStr := fmt.Sprintf(`
SELECT workspace, nonce, publishtime
FROM %s.%s
WHERE typespace = ? AND namespace = ?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace)

	iter := query.Iter()
	count := 0
	var tempWorkspace string
	var nonceID int64
	var publishTime int64
	workspaceList := make(map[string]wsdb.WorkspaceNonce)
	for iter.Scan(&tempWorkspace, &nonceID, &publishTime) {
		workspaceList[tempWorkspace] = wsdb.WorkspaceNonce{Id: nonceID, PublishTime: publishTime}
		count++
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}

	return count, workspaceList, nil
}

func (nc *noCacheWsdb) wsdbKeyGet(c ether.Ctx, typespace string,
	namespace string, workspace string) (key []byte, nonce wsdb.WorkspaceNonce,
	present bool, err error) {

	defer c.FuncIn("noCacheWsdb::wsdbKeyGet", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
SELECT key, nonce, publishtime
FROM %s.%s
WHERE typespace = ? AND namespace = ? AND workspace = ?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	var nonceID, publishTime int64
	err = query.Scan(&key, &nonceID, &publishTime)
	if err != nil {
		switch err {
		case gocql.ErrNotFound:
			return nil, wsdb.WorkspaceNonceInvalid, false, nil
		default:
			return nil, wsdb.WorkspaceNonceInvalid, false, err
		}
	} else {
		nonce := wsdb.WorkspaceNonce{
			Id:          nonceID,
			PublishTime: publishTime,
		}
		return key, nonce, true, nil
	}
}

func (nc *noCacheWsdb) wsdbKeyDel(c ether.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn("noCacheWsdb::wsdbKeyDel", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
DELETE
FROM %s.%s
WHERE typespace=? AND namespace=? AND workspace=?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	return query.Exec()
}

func (nc *noCacheWsdb) wsdbKeyPut(c ether.Ctx, typespace string,
	namespace string, workspace string,
	key []byte, nonce wsdb.WorkspaceNonce) error {

	defer c.FuncIn("noCacheWsdb::wsdbKeyPut", "%s/%s/%s key: %s nonce: %s", typespace,
		namespace, workspace, hex.EncodeToString(key), nonce.String()).Out()

	qryStr := fmt.Sprintf(`
INSERT INTO %s.%s
(typespace, namespace, workspace, key, nonce, publishtime)
VALUES (?,?,?,?,?,?)`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace, key, nonce.Id, nonce.PublishTime)

	return query.Exec()
}

func (nc *noCacheWsdb) wsdbImmutableGet(c ether.Ctx, typespace string,
	namespace string, workspace string) (immutable bool, present bool, err error) {

	defer c.FuncIn("noCacheWsdb::wsdbImmutableGet", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
SELECT immutable
FROM %s.%s
WHERE typespace = ? AND namespace = ? AND workspace = ?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	err = query.Scan(&immutable)
	if err != nil {
		switch err {
		case gocql.ErrNotFound:
			return false, false, nil
		default:
			return false, false, err
		}
	}
	return immutable, true, nil
}
func (nc *noCacheWsdb) wsdbImmutablePut(c ether.Ctx, typespace string,
	namespace string, workspace string, immutable bool) error {

	defer c.FuncIn("noCacheWsdb::wsdbImmutablePut", "%s/%s/%s immutable: %t",
		typespace, namespace, workspace,
		immutable).Out()

	qryStr := fmt.Sprintf(`
UPDATE %s.%s
SET immutable = ?
WHERE typespace = ? AND namespace = ? AND workspace = ?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, immutable, typespace,
		namespace, workspace)

	return query.Exec()
}

func (nc *noCacheWsdb) wsdbKeyLastWriteTime(c ether.Ctx, typespace string,
	namespace string, workspace string) (int64, error) {

	defer c.FuncIn("noCacheWsdb::wsdbKeyLastWriteTime", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	qryStr := fmt.Sprintf(`
SELECT WRITETIME(key)
FROM %s.%s
WHERE typespace=? AND namespace=? AND workspace=?`, nc.keyspace, nc.cfName)

	query := nc.store.session.Query(qryStr, typespace,
		namespace, workspace)

	var writeTime int64
	if err := query.Scan(&writeTime); err != nil {
		return int64(0), err
	}
	return writeTime, nil
}
