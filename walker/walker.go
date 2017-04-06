// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "fmt"
import "path/filepath"
import "runtime"

import "golang.org/x/net/context"
import "golang.org/x/sync/errgroup"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"

type walkFunc func(context.Context, string, quantumfs.ObjectKey, uint64) error

type ctx struct {
	context.Context
	qctx *quantumfs.Ctx
}

type workerData struct {
	path string
	key  quantumfs.ObjectKey
	size uint64
}

// Walk the workspace hierarchy
func Walk(cq *quantumfs.Ctx, ds quantumfs.DataStore, rootID quantumfs.ObjectKey,
	wf walkFunc) error {

	if rootID.Type() != quantumfs.KeyTypeMetadata {
		return fmt.Errorf(
			"Type of rootID %s is %s instead of KeyTypeMetadata",
			rootID.String(), key2String(rootID))
	}

	buf := testutils.NewSimpleBuffer(nil, rootID)
	if err := ds.Get(cq, rootID, buf); err != nil {
		return err
	}
	testutils.AssertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s",
		key2String(rootID))

	wsr := buf.AsWorkspaceRoot()
	//===============================================
	// NOTE: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	group, groupCtx := errgroup.WithContext(context.Background())
	keyChan := make(chan *workerData, 100)

	c := &ctx{
		Context: groupCtx,
		qctx:    cq,
	}

	// Start Workers
	conc := runtime.NumCPU() / 2
	for i := 0; i < conc; i++ {

		group.Go(func() error {
			return worker(c, keyChan, wf)
		})
	}

	group.Go(func() error {
		defer close(keyChan)
		if err := handleHardLinks(c, ds, wsr.HardlinkEntry(), wf,
			keyChan); err != nil {
			return err
		}

		if err := handleDirectoryEntry(c, "/", ds, wsr.BaseLayer(), wf,
			keyChan); err != nil {
			return err
		}
		return nil
	})

	if err := writeToChan(c, keyChan, "", rootID,
		uint64(buf.Size())); err != nil {
		return err
	}

	return group.Wait()
}

func key2String(key quantumfs.ObjectKey) string {
	switch {

	case key.IsEqualTo(quantumfs.EmptyDirKey):
		return "EmptyDirKey"
	case key.IsEqualTo(quantumfs.EmptyBlockKey):
		return "EmptyBlockKey"
	case key.IsEqualTo(quantumfs.EmptyWorkspaceKey):
		return "EmptyWorkspaceKey"
	case key.IsEqualTo(quantumfs.ZeroKey):
		return "ZeroKey"
	default:
		return key.String()
	}
}

func handleHardLinks(c *ctx, ds quantumfs.DataStore,
	hle quantumfs.HardlinkEntry, wf walkFunc,
	keyChan chan<- *workerData) error {

	for {
		//  Go through all records in this entry.
		for idx := 0; idx < hle.NumEntries(); idx++ {

			hlr := hle.Entry(idx)
			dr := hlr.Record()
			linkPath := dr.Filename()
			if err := handleDirectoryRecord(c, linkPath, ds, dr, wf,
				keyChan); err != nil {
				return err
			}
		}
		// Go to next Entry
		if hle.Next().IsEqualTo(quantumfs.EmptyDirKey) ||
			hle.NumEntries() == 0 {
			break
		}

		key := hle.Next()
		buf := testutils.NewSimpleBuffer(nil, key)
		if err := ds.Get(c.qctx, key, buf); err != nil {
			return err
		}

		testutils.AssertNonZeroBuf(buf,
			"WorkspaceRoot buffer %s",
			key2String(key))

		if err := writeToChan(c, keyChan, "", key,
			uint64(buf.Size())); err != nil {
			return err
		}

		hle = buf.AsHardlinkEntry()
	}
	return nil
}

func handleMultiBlockFile(c *ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc,
	keyChan chan<- *workerData) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	if err := ds.Get(c.qctx, key, buf); err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"MultiBlockFile buffer %s",
		key2String(key))

	if err := writeToChan(c, keyChan, path, key,
		uint64(buf.Size())); err != nil {
		return err
	}

	mbf := buf.AsMultiBlockFile()
	keys := mbf.ListOfBlocks()
	for i, k := range keys {
		if i == len(keys)-1 {
			// Return, since this is last block
			return writeToChan(c, keyChan, path, k,
				uint64(mbf.SizeOfLastBlock()))
		}
		if err := writeToChan(c, keyChan, path, k,
			uint64(mbf.BlockSize())); err != nil {
			return err
		}
	}

	return nil
}

func handleVeryLargeFile(c *ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc,
	keyChan chan<- *workerData) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	if err := ds.Get(c.qctx, key, buf); err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"VeryLargeFile buffer %s",
		key2String(key))

	if err := writeToChan(c, keyChan, path, key,
		uint64(buf.Size())); err != nil {
		return err
	}
	vlf := buf.AsVeryLargeFile()
	for part := 0; part < vlf.NumberOfParts(); part++ {
		if err := handleMultiBlockFile(c, path, ds,
			vlf.LargeFileKey(part), wf, keyChan); err != nil {
			return err
		}
	}

	return nil
}

var totalFilesWalked uint64

func handleDirectoryEntry(c *ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf walkFunc,
	keyChan chan<- *workerData) error {

	buf := testutils.NewSimpleBuffer(nil, key)
	if err := ds.Get(c.qctx, key, buf); err != nil {
		return err
	}

	testutils.AssertNonZeroBuf(buf,
		"DirectoryEntry buffer %s",
		key2String(key))

	if err := writeToChan(c, keyChan, path, key,
		uint64(buf.Size())); err != nil {
		return err
	}

	de := buf.AsDirectoryEntry()
	for i := 0; i < de.NumEntries(); i++ {
		if err := handleDirectoryRecord(c, path, ds,
			de.Entry(i), wf, keyChan); err != nil {
			return err
		}
	}

	return nil
}

func handleDirectoryRecord(c *ctx, path string, ds quantumfs.DataStore,
	dr *quantumfs.DirectRecord, wf walkFunc,
	keyChan chan<- *workerData) error {

	fpath := filepath.Join(path, dr.Filename())

	switch dr.Type() {
	case quantumfs.ObjectTypeSmallFile:
		fallthrough
	case quantumfs.ObjectTypeSymlink:
		return writeToChan(c, keyChan, fpath, dr.ID(), dr.Size())
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		return handleMultiBlockFile(c, fpath,
			ds, dr.ID(), wf, keyChan)
	case quantumfs.ObjectTypeVeryLargeFile:
		return handleVeryLargeFile(c, fpath,
			ds, dr.ID(), wf, keyChan)
	case quantumfs.ObjectTypeDirectoryEntry:
		if !dr.ID().IsEqualTo(quantumfs.EmptyDirKey) {
			return handleDirectoryEntry(c, fpath,
				ds, dr.ID(), wf, keyChan)
		}
	default:
	}

	return nil
}

func worker(c context.Context, keyChan <-chan *workerData, wf walkFunc) error {
	var keyItem *workerData
	for {
		select {
		case <-c.Done():
			return fmt.Errorf("Quiting in worker because at least one " +
				"goroutine failed with an error")
		case keyItem = <-keyChan:
			if keyItem == nil {
				return nil
			}

		}
		if err := wf(c, keyItem.path, keyItem.key,
			keyItem.size); err != nil {
			return err
		}
	}
}

func writeToChan(c context.Context, keyChan chan<- *workerData, p string,
	k quantumfs.ObjectKey, s uint64) error {

	select {
	case <-c.Done():
		return fmt.Errorf("Quiting in writeToChan because at least one " +
			"goroutine failed with an error")
	case keyChan <- &workerData{path: p, key: k, size: s}:
	}
	return nil
}
