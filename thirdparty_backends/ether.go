// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// To avoid compiling in support for ether datastore
// change "!skip_backends" in first line with "ignore"
// You will need to do the same in daemon/Ether_test.go as well.
package thirdparty_backends

import "bytes"
import "fmt"

import "github.com/aristanetworks/ether/blobstore"
import "github.com/aristanetworks/ether/cql"
import "github.com/aristanetworks/ether/filesystem"
import "github.com/aristanetworks/ether/qubit/wsdb"
import "github.com/aristanetworks/quantumfs"

//import "github.com/aristanetworks/quantumfs/qlog"

func init() {
	registerDatastore("ether.filesystem", NewEtherFilesystemStore)
	registerDatastore("ether.cql", NewEtherCqlStore)
	registerWorkspaceDB("ether.cql", NewEtherWorkspaceDB)
}

func NewEtherFilesystemStore(path string) quantumfs.DataStore {

	blobstore, err := filesystem.NewFilesystemStore(path)
	if err != nil {
		fmt.Printf("Failed to init ether.filesystem datastore: %s\n",
			err.Error())
		return nil
	}
	translator := EtherBlobStoreTranslator{blobstore: blobstore}
	return &translator
}

func NewEtherCqlStore(path string) quantumfs.DataStore {

	blobstore, err := cql.NewCqlBlobStore(path)
	if err != nil {
		fmt.Printf("Failed to init ether.cql datastore: %s\n",
			err.Error())
		return nil
	}
	translator := EtherBlobStoreTranslator{blobstore: blobstore}
	return &translator
}

type EtherBlobStoreTranslator struct {
	blobstore blobstore.BlobStore
}

func (ebt *EtherBlobStoreTranslator) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	c.Vlog(qlog.LogDatastore, "---In EtherBlobStoreTranslator::Get")
	defer c.Vlog(qlog.LogDatastore, "Out-- EtherBlobStoreTranslator::Get")

	data, _, err := ebt.blobstore.Get(key.String())

	if err != nil {
		return err
	}

	buf.Set(data, key.Type())
	return nil
}

func (ebt *EtherBlobStoreTranslator) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	c.Vlog(qlog.LogDatastore, "---In EtherBlobStoreTranslator::Set")
	defer c.Vlog(qlog.LogDatastore, "Out-- EtherBlobStoreTranslator::Set")

	return ebt.blobstore.Insert(key.String(), buf.Get(), nil)
}

type EtherWsdbTranslator struct {
	wsdb wsdb.WorkspaceDB
}

// convert wsdb.Error to quantumfs.WorkspaceDbErr
func convertWsdbError(e error) error {
	wE, ok := e.(*wsdb.Error)
	if !ok {
		panic("BUG: Errors from wsdb APIs must be of *wsdb.Error type")
	}

	var errCode quantumfs.WsdbErrCode
	switch wE.Code {
	case wsdb.ErrWorkspaceExists:
		errCode = quantumfs.WSDB_WORKSPACE_EXISTS
	case wsdb.ErrWorkspaceNotFound:
		errCode = quantumfs.WSDB_WORKSPACE_NOT_FOUND
	case wsdb.ErrFatal:
		errCode = quantumfs.WSDB_FATAL_DB_ERROR
	case wsdb.ErrWorkspaceOutOfDate:
		errCode = quantumfs.WSDB_OUT_OF_DATE
	case wsdb.ErrLocked:
		errCode = quantumfs.WSDB_LOCKED
	default:
		panic(fmt.Sprintf("Bug: Unsupported error %s", e.Error()))
	}

	return quantumfs.NewWorkspaceDbErr(errCode, wE.Msg)
}

func NewEtherWorkspaceDB(path string) quantumfs.WorkspaceDB {
	eWsdb := &EtherWsdbTranslator{
		wsdb: cql.NewWorkspaceDB(path),
	}

	// since generic wsdb API sets up _null/null with nil key
	key, err := eWsdb.wsdb.AdvanceWorkspace(quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		[]byte(nil), quantumfs.EmptyWorkspaceKey.Value())
	if err != nil {
		// an existing workspaceDB will have currentRootID as
		// EmptyWorkspaceKey
		wE, _ := err.(*wsdb.Error)
		if wE.Code != wsdb.ErrWorkspaceOutOfDate ||
			!bytes.Equal(key, quantumfs.EmptyWorkspaceKey.Value()) {
			panic(fmt.Sprintf("Failed wsdb setup: %s", err.Error()))
		}
	}

	return eWsdb
}

func (w *EtherWsdbTranslator) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::NumTypespaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::NumTypespaces")

	count, err := w.wsdb.NumTypespaces()
	if err != nil {
		return 0, convertWsdbError(err)
	}
	return count, nil
}

func (w *EtherWsdbTranslator) TypespaceList(
	c *quantumfs.Ctx) ([]string, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::TypespaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::TypespaceList")

	list, err := w.wsdb.TypespaceList()
	if err != nil {
		return nil, convertWsdbError(err)
	}
	return list, nil
}

func (w *EtherWsdbTranslator) NumNamespaces(c *quantumfs.Ctx,
	typespace string) (int, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::NumNamespaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::NumNamespaces")

	count, err := w.wsdb.NumNamespaces(typespace)
	if err != nil {
		return 0, convertWsdbError(err)
	}
	return count, nil
}

func (w *EtherWsdbTranslator) NamespaceList(c *quantumfs.Ctx,
	typespace string) ([]string, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::NamespaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::NamespaceList")

	list, err := w.wsdb.NamespaceList(typespace)
	if err != nil {
		return nil, convertWsdbError(err)
	}
	return list, nil
}

func (w *EtherWsdbTranslator) NumWorkspaces(c *quantumfs.Ctx,
	typespace string, namespace string) (int, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::NumWorkspaces")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::NumWorkspaces")

	count, err := w.wsdb.NumWorkspaces(typespace, namespace)
	if err != nil {
		return 0, convertWsdbError(err)
	}
	return count, nil
}

func (w *EtherWsdbTranslator) WorkspaceList(c *quantumfs.Ctx,
	typespace string, namespace string) ([]string, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::WorkspaceList")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::WorkspaceList")

	list, err := w.wsdb.WorkspaceList(typespace, namespace)
	if err != nil {
		return nil, convertWsdbError(err)
	}
	return list, nil
}

func (w *EtherWsdbTranslator) TypespaceExists(c *quantumfs.Ctx,
	typespace string) (bool, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::TypespaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::TypespaceExists")

	exists, err := w.wsdb.TypespaceExists(typespace)
	if err != nil {
		return exists, convertWsdbError(err)
	}
	return exists, nil
}

func (w *EtherWsdbTranslator) NamespaceExists(c *quantumfs.Ctx,
	typespace string, namespace string) (bool, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::NamespaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::NamespaceExists")

	exists, err := w.wsdb.NamespaceExists(typespace, namespace)
	if err != nil {
		return exists, convertWsdbError(err)
	}
	return exists, nil
}

func (w *EtherWsdbTranslator) WorkspaceExists(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::WorkspaceExists")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::WorkspaceExists")

	exists, err := w.wsdb.WorkspaceExists(typespace, namespace, workspace)
	if err != nil {
		return exists, convertWsdbError(err)
	}
	return exists, nil
}

func (w *EtherWsdbTranslator) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::Workspace")
	defer c.Vlog(qlog.LogWorkspaceDb, "Out-- EtherWsdbTranslator::Workspace")

	key, err := w.wsdb.Workspace(typespace, namespace, workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, convertWsdbError(err)
	}

	return quantumfs.NewObjectKeyFromBytes(key), nil
}

func (w *EtherWsdbTranslator) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::BranchWorkspace")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::BranchWorkspace")

	err := w.wsdb.BranchWorkspace(srcTypespace, srcNamespace, srcWorkspace,
		dstTypespace, dstNamespace, dstWorkspace)
	if err != nil {
		return convertWsdbError(err)
	}
	return nil
}

func (w *EtherWsdbTranslator) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::DeleteWorkspace")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::DeleteWorkspace")

	return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_FATAL_DB_ERROR,
		"Ether does not support deleting workspaces")
}

func (w *EtherWsdbTranslator) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	c.Vlog(qlog.LogWorkspaceDb, "---In EtherWsdbTranslator::AdvanceWorkspace")
	defer c.Vlog(qlog.LogWorkspaceDb,
		"Out-- EtherWsdbTranslator::AdvanceWorkspace")

	key, err := w.wsdb.AdvanceWorkspace(typespace, namespace, workspace,
		currentRootId.Value(), newRootId.Value())
	if err != nil {
		return quantumfs.ObjectKey{}, convertWsdbError(err)
	}

	return quantumfs.NewObjectKeyFromBytes(key), nil
}
