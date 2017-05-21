// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "errors"
import "fmt"
import "path/filepath"
import "runtime"
import "runtime/debug"
import "strings"

import "golang.org/x/net/context"
import "golang.org/x/sync/errgroup"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/utils/simplebuffer"
import "github.com/aristanetworks/quantumfs/utils/aggregatedatastore"

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
	qctx *quantumfs.Ctx
}

type workerData struct {
	path string
	key  quantumfs.ObjectKey
	size uint64
}

func panicHandler(result *string) {
	err := recover()
	trace := ""

	// If the test passed pass that fact back to runTest()
	if err == nil {
		err = ""
	} else {
		// Capture the stack trace of the failure
		trace = utils.BytesToString(debug.Stack())
		trace = strings.SplitN(trace, "\n", 8)[7]
	}

	switch err.(type) {
	default:
		*result = fmt.Sprintf("Unknown panic type: %v", err)
	case string:
		*result = err.(string)
	case error:
		*result = err.(error).Error()
	}

	if trace != "" {
		*result += "\nStack Trace:\n" + trace
	}
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
	var result string

	c := &Ctx{
		Context: groupCtx,
		qctx:    cq,
	}

	// Start Workers
	conc := runtime.GOMAXPROCS(-1)
	for i := 0; i < conc; i++ {

		group.Go(func() error {
			defer panicHandler(&result)
			return worker(c, keyChan, wf)
		})
	}

	group.Go(func() error {
		defer panicHandler(&result)
		defer close(keyChan)

		if err := writeToChan(c, keyChan, "", rootID,
			uint64(buf.Size())); err != nil {
			return err
		}

		if err := handleHardLinks(c, ads, wsr.HardlinkEntry(), wf,
			keyChan); err != nil {
			return err
		}

		// Skip WSR
		if err := handleDirectoryEntry(c, "/", ads, wsr.BaseLayer(), wf,
			keyChan); err != nil {
			if err == SkipDir {
				return nil
			}
			return err
		}
		return nil
	})

	err := group.Wait()
	if result != "" {
		err = fmt.Errorf("panic in walker")
		fmt.Println(result)
	}
	return err
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
		if err := ds.Get(c.qctx, key, buf); err != nil {
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
	if err := ds.Get(c.qctx, key, buf); err != nil {
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
	if err := ds.Get(c.qctx, key, buf); err != nil {
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
		if err := ds.Get(c.qctx, key, buf); err != nil {
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
	case quantumfs.ObjectTypeDirectory:
		return handleDirectoryEntry(c, fpath,
			ds, dr.ID(), wf, keyChan)
	default:
	}

	return nil
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

	if err := cds.Get(c.qctx, key, buf); err != nil {
		return false // Not a ConstKey, so do not Skip.
	}
	return true
}
