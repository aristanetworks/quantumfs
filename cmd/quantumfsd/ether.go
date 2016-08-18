// +build linux

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Enable this file by replacing "ignore" from the first line with "linux" if you
// want to compile in support for the ether datastores.
package main

import "fmt"

import "github.com/aristanetworks/ether"
import "github.com/aristanetworks/ether/filesystem"
import "github.com/aristanetworks/quantumfs"

func init() {
	ether := datastore{
		name:        "ether.filesystem",
		constructor: NewEtherFilesystemStore,
	}

	datastores = append(datastores, ether)
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
	if err.ErrorCode != ether.ErrOk {
		return true
	} else {
		return false
	}
}
