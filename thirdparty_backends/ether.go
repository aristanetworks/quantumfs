// +build linux

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Disable this file by replacing "linux" from the first line with "ignore" if you
// want to compile in support for the ether datastores. You will need to do the same
// in daemon/Ether_test.go as well.
package thirdparty_backends

import "fmt"

import "github.com/aristanetworks/ether"
import "github.com/aristanetworks/ether/filesystem"
import "github.com/aristanetworks/quantumfs"

func init() {
	registerDatastore("ether.filesystem", NewEtherFilesystemStore)
}

func NewEtherFilesystemStore(path string) quantumfs.DataStore {
	blobstore, err := filesystem.NewFilesystemStore(path)
	if err.ErrorCode != ether.ErrOk {
		fmt.Printf("Failed to init ether.filesystem datastore: %s\n",
			err.Error())
	}
	translator := EtherBlobStoreTranslator{blobstore: blobstore}
	return &translator
}

type EtherBlobStoreTranslator struct {
	blobstore ether.BlobStore
}

func (ebt *EtherBlobStoreTranslator) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	data, _, err := ebt.blobstore.Get(key.String())

	if err.ErrorCode != ether.ErrOk {
		return &err
	}

	buf.Set(data, key.Type())
	return nil
}

func (ebt *EtherBlobStoreTranslator) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	err := ebt.blobstore.Insert(key.String(), buf.Get(), nil)
	if err.ErrorCode != ether.ErrOk {
		return &err
	} else {
		return nil
	}
}

func (ebt *EtherBlobStoreTranslator) Exists(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) bool {

	_, _, err := ebt.blobstore.Get(key.String())
	if err.ErrorCode == ether.ErrOk {
		return true
	} else {
		return false
	}
}
