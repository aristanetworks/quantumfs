// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/utils/aggregatedatastore"
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// SkipDir is used as a return value from WalkFunc to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var SkipDir = errors.New("skip this directory")

// WalkFunc is the type of the function called for each data block under the
// Workspace.
//
// NOTE
// Walker in this package honors SkipDir only when walkFunc is called for a
// directory.
//
// This is a key difference from path/filepath.Walk,
// in which if filepath.Walkunc returns SkipDir when invoked on a
// non-directory file, Walk skips the remaining files in the
// containing directory.
type WalkFunc func(ctx *Ctx, path string, key quantumfs.ObjectKey,
	size uint64, isDir bool) error

// Ctx maintains context for the walker library.
type Ctx struct {
	context.Context
	Qctx *quantumfs.Ctx
}

type workerData struct {
	path string
	key  quantumfs.ObjectKey
	size uint64
}

func panicHandler(c *Ctx, err *error) {
	exception := recover()
	if exception == nil {
		return
	}

	var result string
	switch exception.(type) {
	default:
		result = fmt.Sprintf("Unknown panic type: %v", exception)
		*err = fmt.Errorf("%s", result)
	case string:
		result = exception.(string)
		*err = fmt.Errorf(result)
	case error:
		result = exception.(error).Error()
		*err = exception.(error)
	}

	trace := utils.BytesToString(debug.Stack())
	c.Qctx.Elog(qlog.LogTool, "%s\n%v", result, trace)
}

// Walk the workspace hierarchy
func Walk(cq *quantumfs.Ctx, ds quantumfs.DataStore, rootID quantumfs.ObjectKey,
	wf WalkFunc) error {

	// encompass the provided datastore in an
	// AggregateDataStore
	ads := aggregatedatastore.New(ds)

	buf := simplebuffer.New(nil, rootID)
	if err := ads.Get(cq, rootID, buf); err != nil {
		return err
	}
	simplebuffer.AssertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s", rootID.Text())

	wsr := buf.AsWorkspaceRoot()
	//===============================================
	// NOTE: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	group, groupCtx := errgroup.WithContext(context.Background())
	keyChan := make(chan *workerData, 100)

	c := &Ctx{
		Context: groupCtx,
		Qctx:    cq,
	}

	// Start Workers
	conc := runtime.GOMAXPROCS(-1)
	for i := 0; i < conc; i++ {

		group.Go(func() (err error) {
			defer panicHandler(c, &err)
			err = worker(c, keyChan, wf)
			return
		})
	}

	group.Go(func() (err error) {
		defer panicHandler(c, &err)
		defer close(keyChan)

		if err = writeToChan(c, keyChan, "", rootID,
			uint64(buf.Size())); err != nil {
			return
		}

		if err = handleHardLinks(c, ads, wsr.HardlinkEntry(), wf,
			keyChan); err != nil {
			return
		}

		// Skip WSR
		if err = handleDirectoryEntry(c, "/", ads, wsr.BaseLayer(), wf,
			keyChan); err != nil {
			if err == SkipDir {
				return nil
			}
			return
		}
		return
	})
	return group.Wait()
}

func handleHardLinks(c *Ctx, ds quantumfs.DataStore,
	hle quantumfs.HardlinkEntry, wf WalkFunc,
	keyChan chan<- *workerData) error {

	for {
		// Go through all records in this entry.
		for idx := 0; idx < hle.NumEntries(); idx++ {

			hlr := hle.Entry(idx)
			dr := hlr.Record()
			linkPath := dr.Filename()
			if err := handleDirectoryRecord(c, linkPath, ds, dr, wf,
				keyChan); err != nil {
				return err
			}
		}
		if !hle.HasNext() {
			break
		}

		key := hle.Next()
		buf := simplebuffer.New(nil, key)
		if err := ds.Get(c.Qctx, key, buf); err != nil {
			return err
		}

		simplebuffer.AssertNonZeroBuf(buf,
			"WorkspaceRoot buffer %s", key.Text())

		if err := writeToChan(c, keyChan, "", key,
			uint64(buf.Size())); err != nil {
			return err
		}

		hle = buf.AsHardlinkEntry()
	}
	return nil
}

func handleMultiBlockFile(c *Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf WalkFunc,
	keyChan chan<- *workerData) error {

	buf := simplebuffer.New(nil, key)
	if err := ds.Get(c.Qctx, key, buf); err != nil {
		return err
	}

	simplebuffer.AssertNonZeroBuf(buf,
		"MultiBlockFile buffer %s", key.Text())

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

func handleVeryLargeFile(c *Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf WalkFunc,
	keyChan chan<- *workerData) error {

	buf := simplebuffer.New(nil, key)
	if err := ds.Get(c.Qctx, key, buf); err != nil {
		return err
	}

	simplebuffer.AssertNonZeroBuf(buf,
		"VeryLargeFile buffer %s", key.Text())

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

func handleDirectoryEntry(c *Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, wf WalkFunc,
	keyChan chan<- *workerData) error {

	for {
		buf := simplebuffer.New(nil, key)
		if err := ds.Get(c.Qctx, key, buf); err != nil {
			return err
		}

		simplebuffer.AssertNonZeroBuf(buf,
			"DirectoryEntry buffer %s", key.Text())

		// When wf returns SkipDir for a DirectoryEntry, we can skip all the
		// DirectoryRecord in that DirectoryEntry
		if err := wf(c, path, key, uint64(buf.Size()), true); err != nil {
			// TODO(sid): See how this works with chain DirectoryEntries.
			//            Since we check only the first of the many
			//            chained DiretoryEntries.
			if err == SkipDir {
				return nil
			}
			return err
		}

		de := buf.AsDirectoryEntry()
		for i := 0; i < de.NumEntries(); i++ {
			if err := handleDirectoryRecord(c, path, ds,
				de.Entry(i), wf, keyChan); err != nil {
				return err
			}
		}
		if !de.HasNext() {
			break
		}

		key = de.Next()
	}
	return nil
}

func handleDirectoryRecord(c *Ctx, path string, ds quantumfs.DataStore,
	dr *quantumfs.DirectRecord, wf WalkFunc,
	keyChan chan<- *workerData) error {

	fpath := filepath.Join(path, dr.Filename())

	switch dr.Type() {
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		return handleMultiBlockFile(c, fpath,
			ds, dr.ID(), wf, keyChan)
	case quantumfs.ObjectTypeVeryLargeFile:
		return handleVeryLargeFile(c, fpath,
			ds, dr.ID(), wf, keyChan)
	case quantumfs.ObjectTypeDirectory:
		return handleDirectoryEntry(c, fpath,
			ds, dr.ID(), wf, keyChan)
		// The default case handles the following as well:
		// quantumfs.ObjectTypeSpecial:
		// quantumfs.ObjectTypeSmallFile:
		// quantumfs.ObjectTypeSymlink:
	default:
		return writeToChan(c, keyChan, fpath, dr.ID(), dr.Size())
	}
}

func worker(c *Ctx, keyChan <-chan *workerData, wf WalkFunc) error {
	var keyItem *workerData
	for {
		select {
		case <-c.Done():
			return fmt.Errorf("Quitting worker because at least one " +
				"goroutine failed with an error")
		case keyItem = <-keyChan:
			if keyItem == nil {
				return nil
			}
		}
		if err := wf(c, keyItem.path, keyItem.key,
			keyItem.size, false); err != nil {
			return err
		}
	}
}

func writeToChan(c context.Context, keyChan chan<- *workerData, p string,
	k quantumfs.ObjectKey, s uint64) error {

	select {
	case <-c.Done():
		return fmt.Errorf("Quitting writeToChan because at least one " +
			"goroutine failed with an error")
	case keyChan <- &workerData{path: p, key: k, size: s}:
	}
	return nil
}

// SkipKey returns true:
// If the Key is in Constant DataStore, or
// If the Key is of Type Embedded,
func SkipKey(c *Ctx, key quantumfs.ObjectKey) bool {

	if key.Type() == quantumfs.KeyTypeEmbedded {
		return true
	}

	cds := quantumfs.ConstantStore
	buf := simplebuffer.New(nil, key)

	if err := cds.Get(c.Qctx, key, buf); err != nil {
		return false // Not a ConstKey, so do not Skip.
	}
	return true
}
