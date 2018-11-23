// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

func NewDataStore(conf string) quantumfs.DataStore {
	os.MkdirAll(conf, 0600)

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

	path := filepath.Join(root, hash[2:4], hash[4:6], hash)
	return path
}

func (ds *datastore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Get", "key: %s", ks).Out()

	path := objectPath(ds.dbPath, key)

	file, err := os.Open(path)
	if err != nil {
		c.Vlog(qlog.LogDatastore, "object file not available: %s",
			err.Error())
		return nil
	}
	defer file.Close()

	data := make([]byte, quantumfs.MaxBlockSize)
	size, err := file.Read(data)
	if err != nil {
		c.Vlog(qlog.LogDatastore, "Failed reading object: %s", err.Error())
		return nil
	}
	c.Vlog(qlog.LogDatastore, "Read %d bytes", size)
	data = data[:size]

	buf.Set(data, key.Type())

	return nil
}

func (ds *datastore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Set", "key: %s", ks).Out()

	path := objectPath(ds.dbPath, key)
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, 0600)
	if err != nil {
		c.Wlog(qlog.LogDatastore,
			"Failed to create hash directory %s: %s", dir, err.Error())
		return err
	}

	file, err := ioutil.TempFile(dir, "newobject")
	if err != nil {
		c.Wlog(qlog.LogDatastore, "Failed creating temp file in %s: %s",
			dir, err.Error())
		return err
	}
	defer file.Close()

	size, err := file.Write(buf.Get())
	if err != nil {
		c.Wlog(qlog.LogDatastore, "Failed to write new object %s: %s",
			path, err.Error())
		return err
	}
	c.Vlog(qlog.LogDatastore, "Wrote new object of %d bytes", size)

	err = os.Rename(file.Name(), path)
	if err != nil {
		c.Dlog(qlog.LogDatastore, "Object rename failed: %s", err.Error())
		// Assume somebody beat us and the object is saved
		err = os.Remove(file.Name())
		if err != nil {
			c.Elog(qlog.LogDatastore, "Object race cleanup failed: %s",
				err.Error())
		}
	}

	return nil
}

func (ds *datastore) Freshen(c *quantumfs.Ctx, key quantumfs.ObjectKey) error {
	ks := key.String()
	defer c.FuncIn(qlog.LogDatastore, "systemlocal::Freshen", "key: %s",
		ks).Out()

	return nil
}
