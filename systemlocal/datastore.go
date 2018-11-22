// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

import (
	"encoding/hex"
	"path/filepath"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

func NewDatastore(conf string) quantumfs.DataStore {
	return &datastore{
		dbPath: conf,
	}
}

// datastore is a simple persistent, filesystem local quantumfs.DataStore.
//
// The basic structure is one of nested directories named after hash prefixes which
// contain the file with the full has as the filename within the appropriate
// directory.
//
// Note that the prefix directories are named after key.Hash(), not key.Value()
// becuase the latter contains the key type, which is not expected to be evently
// distributed.
type datastore struct {
	dbPath string
}

func objectPath(root string, key quantumfs.ObjectKey) string {
	hash := hex.EncodeToString(key.Value())

	path := filepath.Join(root, hash[2:6], hash[6:10], hash)
	return path
}

func (ds *datastore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Get", "key: %s", ks).Out()

	_ = objectPath(ds.dbPath, key)

	return nil
}

func (ds *datastore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Set", "key: %s", ks).Out()

	return nil
}

func (ds *datastore) Freshen(c *quantumfs.Ctx, key quantumfs.ObjectKey) error {
	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Freshen", "key: %s",
		ks).Out()

	return nil
}
