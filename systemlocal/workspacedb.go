// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/boltdb/bolt"
)

var typespacesBucket []byte

func init() {
	typespacesBucket = []byte("Typespaces")
}

type workspaceInfo struct {
	Key       string // hex encoded ObjectKey.Value()
	Nonce     quantumfs.WorkspaceNonce
	Immutable bool
}

func encodeKey(key quantumfs.ObjectKey) string {
	return hex.EncodeToString(key.Value())
}

func decodeKey(key string) quantumfs.ObjectKey {
	if key == "" {
		return quantumfs.ZeroKey
	}

	data, err := hex.DecodeString(key)
	utils.Assert(err == nil, "Error decoding string '%s' from database: %v", key,
		err)
	return quantumfs.NewObjectKeyFromBytes(data)
}

// The database underlying the system local workspaceDB has three levels of buckets.
//
// The first level bucket are all the typespaces. Keys into this bucket are typespace
// names and are mapped to the second level bucket.
//
// The second level bucket are all the namespaces. Keys into this bucket are
// namespace names and are mapped to the third level bucket.
//
// The third level of buckets are the workspaces within each namespace.
//
// This structure maps naturally to the heirarchical nature of workspace naming and
// helps us to perform namespace and workspace counts easily.

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	var options *bolt.Options

	if strings.HasPrefix(conf, "/tmp") {
		// We are running inside a test, don't wait forever
		options = &bolt.Options{
			Timeout: 100 * time.Millisecond,
		}
	}

	db, err := bolt.Open(conf, 0600, options)
	if err != nil {
		panic(err.Error())
	}

	if strings.HasPrefix(conf, "/tmp") {
		// We are running inside a test, syncing can only slow us down
		db.NoSync = true
	}

	wsdb := &workspaceDB{
		db:            db,
		callback:      nil,
		updates:       map[string]quantumfs.WorkspaceState{},
		subscriptions: map[string]bool{},
	}

	nullWorkspace := workspaceInfo{
		Key:       encodeKey(quantumfs.EmptyWorkspaceKey),
		Nonce:     quantumfs.WorkspaceNonce{},
		Immutable: true,
	}

	// Create the null workspace
	err = db.Update(func(tx *bolt.Tx) error {
		return setWorkspaceInfo_(tx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			nullWorkspace)
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create null workspace: %s",
			err.Error()))
	}

	return wsdb
}

// workspaceDB is a persistent, system local quantumfs.WorkspaceDB. It only supports
// one Quantumfs instance at a time however.
type workspaceDB struct {
	db *bolt.DB

	lock          utils.DeferableMutex
	callback      quantumfs.SubscriptionCallback
	updates       map[string]quantumfs.WorkspaceState
	subscriptions map[string]bool
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	var num int

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumTypespaces").Out()

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)

		typespaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	typespaceList := make([]string, 0, 100)

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::TypespaceList").Out()

	wsdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(typespacesBucket)
		typespaces := bucket.Cursor()
		for n, _ := typespaces.First(); n != nil; n, _ = typespaces.Next() {
			typespaceList = append(typespaceList, string(n))
		}

		return nil
	})

	return typespaceList, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumNamespaces").Out()

	var num int

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		if namespaces == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Typespace %s does not exist",
				typespace)
		}

		namespaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, err
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NamespaceList").Out()

	namespaceList := make([]string, 0, 100)

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		bucket := typespaces.Bucket([]byte(typespace))
		if bucket == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Typespace %s does not exist",
				typespace)
		}

		namespaces := bucket.Cursor()
		for n, _ := namespaces.First(); n != nil; n, _ = namespaces.Next() {
			namespaceList = append(namespaceList, string(n))
		}

		return nil
	})

	return namespaceList, err
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumWorkspaces").Out()

	var num int

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		if namespaces == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Typespace %s does not exist",
				typespace)
		}

		workspaces := namespaces.Bucket([]byte(namespace))
		if workspaces == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Namespace %s does not exist",
				namespace)
		}

		workspaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, err
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) (map[string]quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::WorkspaceList").Out()

	workspaceList := make(map[string]quantumfs.WorkspaceNonce, 100)

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		if namespaces == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Typespace %s does not exist",
				typespace)
		}

		bucket := namespaces.Bucket([]byte(namespace))
		if bucket == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Namespace %s does not exist",
				namespace)
		}

		workspaces := bucket.Cursor()
		name, infoBytes := workspaces.First()
		for ; name != nil; name, infoBytes = workspaces.Next() {
			info := bytesToInfo(infoBytes, string(name))
			workspaceList[string(name)] = info.Nonce
		}

		return nil
	})

	return workspaceList, err
}

func bytesToInfo(str []byte, workspace string) workspaceInfo {
	var info workspaceInfo
	err := json.Unmarshal(str, &info)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode workspaceInfo for %s: %s '%s'",
			workspace, err.Error(), string(str)))
	}

	return info
}

// Get the workspace key. This must be run inside a boltDB transaction
func getWorkspaceInfo_(tx *bolt.Tx, typespace string, namespace string,
	workspace string) *workspaceInfo {

	typespaces := tx.Bucket(typespacesBucket)
	namespaces := typespaces.Bucket([]byte(typespace))
	if namespaces == nil {
		// The typespace doesn't exist, so the namespace cannot exist
		return nil
	}

	workspaces := namespaces.Bucket([]byte(namespace))
	if workspaces == nil {
		// The namespace doesn't exist, so the workspace cannot exist
		return nil
	}

	encoded := workspaces.Get([]byte(workspace))

	if encoded == nil {
		return nil
	}

	info := bytesToInfo(encoded, workspace)
	return &info
}

// Set the workspace key. This must be run inside a boltDB transaction
func setWorkspaceInfo_(tx *bolt.Tx, typespace string, namespace string,
	workspace string, info workspaceInfo) error {

	typespaces, err := tx.CreateBucketIfNotExists(typespacesBucket)
	if err != nil {
		return fmt.Errorf("Unable to create typespaces bucket: %s",
			err.Error())
	}

	typeBucket, err := typespaces.CreateBucketIfNotExists([]byte("" +
		typespace))
	if err != nil {
		return fmt.Errorf("Unable to create typespace: %s: %s", typespace,
			err.Error())
	}

	nameBucket, err := typeBucket.CreateBucketIfNotExists([]byte("" +
		namespace))
	if err != nil {
		return fmt.Errorf("Unable to create namespace %s/%s: %s", typespace,
			namespace, err.Error())
	}

	encoded, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("Unable to encode workspace info: %s", err.Error())
	}

	err = nameBucket.Put([]byte(workspace), encoded)
	if err != nil {
		return fmt.Errorf("Unable to set workspace %s/%s/%s: %s", typespace,
			namespace, workspace, err.Error())
	}

	return nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::BranchWorkspace").Out()

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		// Get the source workspace rootID
		srcInfo := getWorkspaceInfo_(tx, srcTypespace, srcNamespace,
			srcWorkspace)
		if srcInfo == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Source Workspace %s/%s/%s does not exist",
				srcTypespace, srcNamespace, srcWorkspace)
		}

		dstInfo := getWorkspaceInfo_(tx, dstTypespace, dstNamespace,
			dstWorkspace)
		if dstInfo != nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_EXISTS,
				"Workspace already exists %s/%s/%s",
				dstTypespace, dstNamespace,
				dstWorkspace)
		}

		newInfo := workspaceInfo{
			Key: srcInfo.Key,
			Nonce: quantumfs.WorkspaceNonce{
				uint64(time.Now().UnixNano()), 0},
			Immutable: false,
		}

		err := setWorkspaceInfo_(tx, dstTypespace, dstNamespace,
			dstWorkspace, newInfo)

		if c != nil {
			c.Dlog(qlog.LogWorkspaceDb,
				"Branch workspace '%s/%s/%s' to '%s/%s/%s' with %s",
				srcTypespace, srcNamespace, srcWorkspace,
				dstTypespace, dstNamespace, dstWorkspace,
				decodeKey(srcInfo.Key).String())
		}

		return err
	})
}

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncIn(qlog.LogWorkspaceDb, "systemlocal::DeleteWorkspace",
		"%s/%s/%s", typespace, namespace, workspace).Out()

	if len(typespace) == 0 || len(namespace) == 0 || len(workspace) == 0 {
		return quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_FATAL_DB_ERROR,
			"Malformed workspace name '%s/%s/%s'", typespace, namespace,
			workspace)
	}

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		if namespaces == nil {
			// No such typespace indicates no such workspace. Success.
			return nil
		}

		workspaces := namespaces.Bucket([]byte(namespace))
		if workspaces == nil {
			// No such namespace indicates no such workspace. Success.
			return nil
		}

		err := workspaces.Delete([]byte(workspace))
		if err != nil {
			return err
		}

		var count int
		workspaces.ForEach(func(k []byte, v []byte) error {
			count++
			return nil
		})

		if count == 0 {
			err = namespaces.DeleteBucket([]byte(namespace))
			if err != nil {
				return err
			}
		}

		count = 0
		namespaces.ForEach(func(k []byte, v []byte) error {
			count++
			return nil
		})

		if count == 0 {
			err = typespaces.DeleteBucket([]byte(typespace))
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey,
	quantumfs.WorkspaceNonce, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::Workspace").Out()

	var rootid string
	var nonce quantumfs.WorkspaceNonce

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		info := getWorkspaceInfo_(tx, typespace, namespace, workspace)

		if info == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"workspace does not exist")
		}

		rootid = info.Key
		nonce = info.Nonce

		return nil
	})

	return decodeKey(rootid), nonce, err
}

func (wsdb *workspaceDB) FetchAndSubscribeWorkspace(c *quantumfs.Ctx,
	typespace string, namespace string, workspace string) (
	quantumfs.ObjectKey, quantumfs.WorkspaceNonce, error) {

	err := wsdb.SubscribeTo(typespace + "/" + namespace + "/" + workspace)
	if err != nil {
		return quantumfs.ZeroKey, quantumfs.WorkspaceNonce{}, err
	}

	return wsdb.Workspace(c, typespace, namespace, workspace)
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, nonce quantumfs.WorkspaceNonce,
	currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb,
		"systemlocal::AdvanceWorkspace").Out()

	var dbRootId string

	err := wsdb.db.Update(func(tx *bolt.Tx) error {
		info := getWorkspaceInfo_(tx, typespace, namespace, workspace)
		if info == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Advance failed")
		}

		if !nonce.Equals(&info.Nonce) {
			e := quantumfs.NewWorkspaceDbErr(quantumfs.WSDB_OUT_OF_DATE,
				"Nonce %d does not match WSDB (%d)", nonce,
				info.Nonce)
			return e
		}

		if encodeKey(currentRootId) != info.Key {
			dbRootId = info.Key
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_OUT_OF_DATE,
				"%s vs %s Advance failed", currentRootId.String(),
				decodeKey(dbRootId).String())

		}

		if c != nil {
			c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 3,
				"WorkspaceDB::AdvanceWorkspace %s/%s/%s %s",
				typespace, namespace, workspace, newRootId.String())
		}

		// The workspace exists and the caller has the uptodate rootid, so
		// advance the rootid in the DB.
		info.Key = encodeKey(newRootId)
		info.Nonce.Iteration++
		err := setWorkspaceInfo_(tx, typespace, namespace, workspace, *info)
		if err == nil {
			dbRootId = info.Key
		}
		return err
	})

	return decodeKey(dbRootId), err
}

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	var immutable bool
	err := wsdb.db.View(func(tx *bolt.Tx) error {
		info := getWorkspaceInfo_(tx, typespace, namespace, workspace)
		if info == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Workspace not found")
		}
		immutable = info.Immutable
		return nil
	})

	return immutable, err
}

// Must be called from a BoltDB transaction
func getWorkspaces_(tx *bolt.Tx, typespace string,
	namespace string) (*bolt.Bucket, error) {

	typespaces := tx.Bucket(typespacesBucket)

	namespaces, err := typespaces.CreateBucketIfNotExists([]byte(typespace))
	if err != nil {
		return nil, err
	}

	workspaces, err := namespaces.CreateBucketIfNotExists([]byte(namespace))
	if err != nil {
		return nil, err
	}
	return workspaces, nil
}

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		info := getWorkspaceInfo_(tx, typespace, namespace,
			workspace)
		if info == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Workspace not found")
		}

		info.Immutable = true

		err := setWorkspaceInfo_(tx, typespace, namespace, workspace, *info)

		return err
	})
}

func (wsdb *workspaceDB) SetCallback(callback quantumfs.SubscriptionCallback) {
	defer wsdb.lock.Lock().Unlock()
	wsdb.callback = callback
}

func (wsdb *workspaceDB) SubscribeTo(workspaceName string) error {
	defer wsdb.lock.Lock().Unlock()
	wsdb.subscriptions[workspaceName] = true

	return nil
}

func (wsdb *workspaceDB) UnsubscribeFrom(workspaceName string) {
	defer wsdb.lock.Lock().Unlock()
	delete(wsdb.subscriptions, workspaceName)
}
