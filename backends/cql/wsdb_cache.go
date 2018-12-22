// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs/backends/cql/utils/stats"
	"github.com/aristanetworks/quantumfs/backends/cql/utils/stats/inmem"
)

// In this implementation of workspace DB API,
// some APIs use cached information and do not
// interact with underlying datastore. All APIs interact
// with the cache to maintain local coherency. This
// implementation relies on the uncached workspace DB
// API to interact with underlying datastore.

type cacheWsdb struct {
	base  WorkspaceDB
	cache *entityCache

	branchStats  stats.OpStats
	advanceStats stats.OpStats
}

const defaultCacheTimeoutSecs = 1

// this wsdb implementation wraps any wsdb (base) implementation
// with entity cache
func newCacheWsdb(base WorkspaceDB, cfg WsDBConfig) WorkspaceDB {

	cwsdb := &cacheWsdb{
		base:         base,
		branchStats:  inmem.NewOpStatsInMem("branchWorkspace"),
		advanceStats: inmem.NewOpStatsInMem("advanceWorkspace"),
	}

	cacheTimeout := cfg.CacheTimeoutSecs
	if cacheTimeout == 0 {
		cacheTimeout = defaultCacheTimeoutSecs
	} else if cacheTimeout < 0 && cacheTimeout != DontExpireWsdbCache {
		panic(fmt.Sprintf("Unsupported CacheTimeoutSecs value: %d in "+
			"wsdb configuration\n", cacheTimeout))
	}

	ce := newEntityCache(4, cacheTimeout, cwsdb, wsdbFetcherImpl)
	nonce := WorkspaceNonceInvalid
	ce.InsertEntities(DefaultCtx, NullSpaceName, NullSpaceName,
		NullSpaceName, nonce.String())
	cwsdb.cache = ce
	return cwsdb
}

// --- workspace DB API implementation ---

func (cw *cacheWsdb) NumTypespaces(c Ctx) (int, error) {
	defer c.FuncInName("cacheWsdb::NumTypespaces").Out()

	return cw.cache.CountEntities(c)
}

func (cw *cacheWsdb) TypespaceList(c Ctx) ([]string, error) {
	defer c.FuncInName("cacheWsdb::TypespaceList").Out()

	return cw.cache.ListEntities(c)
}

func (cw *cacheWsdb) NumNamespaces(c Ctx, typespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumNamespaces", "%s", typespace).Out()

	return cw.cache.CountEntities(c, typespace)
}

func (cw *cacheWsdb) NamespaceList(c Ctx,
	typespace string) ([]string, error) {

	defer c.FuncIn("cacheWsdb::NamespaceList", "%s", typespace).Out()

	return cw.cache.ListEntities(c, typespace)
}

func (cw *cacheWsdb) NumWorkspaces(c Ctx, typespace,
	namespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumWorkspaces", "%s/%s",
		typespace, namespace).Out()

	return cw.cache.CountEntities(c, typespace, namespace)
}

func (cw *cacheWsdb) WorkspaceList(c Ctx, typespace string,
	namespace string) (map[string]WorkspaceNonce, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceList", "%s/%s",
		typespace, namespace).Out()

	wsMap := make(map[string]WorkspaceNonce)
	wsList, err := cw.cache.ListEntities(c, typespace, namespace)
	if err != nil {
		return nil, err
	}

	for _, ws := range wsList {
		nonceStr, err := cw.cache.ListEntities(c, typespace, namespace, ws)
		if err != nil {
			return nil, err
		}

		var nonce WorkspaceNonce
		// There should be exactlty 1 nonce for a ts/ns/ws
		if len(nonceStr) != 1 {
			nonce = WorkspaceNonceInvalid
			c.Elog("cacheWsdb::WorkspaceList %d nonces for %s/%s/%s",
				len(nonceStr), typespace, namespace, ws)
		} else {
			nonce, err = StringToNonce(nonceStr[0])
			if err != nil {
				panic(fmt.Sprintf("Nonce is not a valid int64: %s",
					err.Error()))
			}
		}
		wsMap[ws] = nonce
	}

	return wsMap, err
}

func (cw *cacheWsdb) CreateWorkspace(c Ctx, typespace string, namespace string,
	workspace string, nonce WorkspaceNonce, wsKey ObjectKey) error {

	keyHex := hex.EncodeToString(wsKey)
	defer c.FuncIn("cacheWsdb::CreateWorkspace", "%s/%s/%s(%s)(%s)",
		typespace, namespace, workspace, keyHex, nonce.String()).Out()

	err := cw.base.CreateWorkspace(c, typespace, namespace, workspace, nonce,
		wsKey)
	if err != nil {
		return err
	}
	return nil
}

func (cw *cacheWsdb) BranchWorkspace(c Ctx, srcTypespace string, srcNamespace string,
	srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) (WorkspaceNonce,
	WorkspaceNonce, error) {

	start := time.Now()
	defer func() { cw.branchStats.RecordOp(time.Since(start)) }()

	defer c.FuncIn("cacheWsdb::BranchWorkspace", "%s/%s/%s -> %s/%s/%s)",
		srcTypespace, srcNamespace, srcWorkspace, dstTypespace, dstNamespace,
		dstWorkspace).Out()

	srcNonce, dstNonce, err := cw.base.BranchWorkspace(c, srcTypespace,
		srcNamespace, srcWorkspace, dstTypespace, dstNamespace, dstWorkspace)
	if err != nil {
		return WorkspaceNonceInvalid, WorkspaceNonceInvalid, err
	}

	cw.cache.InsertEntities(c, srcTypespace, srcNamespace, srcWorkspace,
		srcNonce.String())
	cw.cache.InsertEntities(c, dstTypespace, dstNamespace, dstWorkspace,
		dstNonce.String())

	return srcNonce, dstNonce, nil
}

func (cw *cacheWsdb) DeleteWorkspace(c Ctx, typespace string, namespace string,
	workspace string) error {

	defer c.FuncIn("cacheWsdb::DeleteWorkspace", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	if err := cw.base.DeleteWorkspace(c, typespace, namespace,
		workspace); err != nil {
		return err
	}

	cw.cache.DeleteEntities(c, typespace, namespace, workspace)
	return nil
}

func (cw *cacheWsdb) WorkspaceLastWriteTime(c Ctx,
	typespace string, namespace string,
	workspace string) (time.Time, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceLastWriteTime", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	ts, err := cw.base.WorkspaceLastWriteTime(c, typespace, namespace,
		workspace)
	if err != nil {
		return time.Time{}, err
	}

	return ts, nil
}

func (cw *cacheWsdb) Workspace(c Ctx, typespace string, namespace string,
	workspace string) (ObjectKey, WorkspaceNonce, error) {

	defer c.FuncIn("cacheWsdb::Workspace", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	key, nonce, err := cw.base.Workspace(c, typespace, namespace, workspace)
	if err != nil {
		return ObjectKey{}, WorkspaceNonceInvalid, err
	}
	cw.cache.InsertEntities(c, typespace, namespace, workspace, nonce.String())
	return key, nonce, nil
}

func (cw *cacheWsdb) AdvanceWorkspace(c Ctx, typespace string,
	namespace string, workspace string, nonce WorkspaceNonce,
	currentRootID ObjectKey,
	newRootID ObjectKey) (ObjectKey, WorkspaceNonce, error) {

	currentKeyHex := hex.EncodeToString(currentRootID)
	newKeyHex := hex.EncodeToString(newRootID)

	defer c.FuncIn("cacheWsdb::AdvanceWorkspace",
		"%s/%s/%s(%s -> %s) old-nonce:%s", typespace, namespace,
		workspace, currentKeyHex, newKeyHex, nonce.String()).Out()

	start := time.Now()
	defer func() { cw.advanceStats.RecordOp(time.Since(start)) }()

	key, nonce, err := cw.base.AdvanceWorkspace(c, typespace, namespace,
		workspace, nonce, currentRootID, newRootID)
	if err != nil {
		return key, nonce, err
	}

	cw.cache.InsertEntities(c, typespace, namespace, workspace, nonce.String())

	return key, nonce, nil
}

func (cw *cacheWsdb) SetWorkspaceImmutable(c Ctx, typespace string, namespace string,
	workspace string) error {

	defer c.FuncIn("cacheWsdb::SetWorkspaceImmutable", "%s/%s/%s",
		typespace, namespace, workspace).Out()

	return cw.base.SetWorkspaceImmutable(c, typespace, namespace, workspace)
}

func (cw *cacheWsdb) WorkspaceIsImmutable(c Ctx, typespace string, namespace string,
	workspace string) (bool, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceIsImmutable", "%s/%s/%s",
		typespace, namespace, workspace).Out()

	immutable, err := cw.base.WorkspaceIsImmutable(c,
		typespace, namespace, workspace)
	c.Vlog("cacheWsdb::WorkspaceIsImmutable %s/%s/%s immutable:%t", typespace,
		namespace, workspace, immutable)
	return immutable, err
}

// wsdbFetcherImpl implements fetcher interface in entity cache
// the returned map contains entities inserted on local node or
// insertions from other nodes in the CQL cluster
func wsdbFetcherImpl(c Ctx, arg interface{},
	entityPath ...string) (map[string]bool, error) {

	cw, ok := arg.(*cacheWsdb)
	if !ok {
		panic("unsupported type of arg")
	}

	var list []string
	var err error
	numEntityParts := len(entityPath)
	switch numEntityParts {
	case 0:
		list, err = cw.base.TypespaceList(c)
	case 1:
		list, err = cw.base.NamespaceList(c, entityPath[0])
	case 2:
		wsMap, err := cw.base.WorkspaceList(c, entityPath[0], entityPath[1])
		if err != nil {
			return nil, err
		}
		// All the keys together make up the list of workspaces
		// needed for the given ts/ns/...
		for wsname := range wsMap {
			list = append(list, wsname)
		}
	case 3:
		// Get the WorkspaceNonce for the given ts/ns/ws
		_, nonce, err := cw.base.Workspace(c, entityPath[0], entityPath[1],
			entityPath[2])
		if err != nil {
			return nil, err
		}
		list = append(list, nonce.String())
	default:
		panic("unsupported entityPath depth")
	}

	m := make(map[string]bool)
	if err != nil {
		return m, err
	}

	for _, val := range list {
		m[val] = true
	}

	return m, nil
}

func (cw *cacheWsdb) ReportAPIStats(c Ctx) {
	defer c.FuncInName("cacheWsdb::ReportAPIStats").Out()

	cw.branchStats.(stats.OpStatReporter).ReportOpStats()
	cw.advanceStats.(stats.OpStatReporter).ReportOpStats()
}
