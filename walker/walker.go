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

// ErrSkipDirectory is a special error from WalkFunc to indicate that
// the directory named in the call is to be skipped.
// This error will not be treated as an error during workspace walk.
var ErrSkipDirectory = errors.New("skip this directory")

// WalkFunc is the type of the function called for each data block under the
// Workspace.
type WalkFunc func(ctx *Ctx, path string, key quantumfs.ObjectKey,
	size uint64, objType quantumfs.ObjectType) error

// Ctx maintains context for the walker library.
type Ctx struct {
	context.Context
	Qctx   *quantumfs.Ctx
	rootID quantumfs.ObjectKey

	hlkeys map[quantumfs.FileId]*quantumfs.EncodedDirectoryRecord
}

type workerData struct {
	path    string
	key     quantumfs.ObjectKey
	size    uint64
	objType quantumfs.ObjectType
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

	var err error
	buf := simplebuffer.New(nil, rootID)
	if err = ads.Get(cq, rootID, buf); err != nil {
		return err
	}
	simplebuffer.AssertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s", rootID.String())

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
		rootID:  rootID,
		hlkeys: make(
			map[quantumfs.FileId]*quantumfs.EncodedDirectoryRecord),
	}

	// Start Workers
	conc := runtime.GOMAXPROCS(-1)
	for i := 0; i < conc; i++ {

		group.Go(func() (err error) {
			defer panicHandler(c, &err)
			err = worker(c, keyChan, wf)
			if err != nil {
				cq.Elog(qlog.LogTool, "Worker error: %s",
					err.Error())
			}
			return err
		})
	}

	group.Go(func() (err error) {
		defer panicHandler(c, &err)
		defer close(keyChan)

		// WSR
		if err = writeToChan(c, keyChan, "[rootId]", rootID,
			uint64(buf.Size()),
			quantumfs.ObjectTypeWorkspaceRoot); err != nil {

			return err
		}

		if err = handleHardLinks(c, ads, wsr.HardlinkEntry(), wf,
			keyChan); err != nil {

			return err
		}

		// all the hardlinks in this workspace must be walked
		// prior to starting the walk of the root directory to
		// enable lookup for fileID in directoryRecord of the
		// path which represents the hardlink

		if err = handleDirectoryEntry(c, "/", ads, wsr.BaseLayer(), wf,
			keyChan); err != nil {

			return err
		}

		return nil
	})

	err = group.Wait()
	if err != nil {
		cq.Elog(qlog.LogTool, "Walk error: %s", err.Error())
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
			err := handleDirectoryRecord(c, linkPath, ds, dr, wf,
				keyChan)
			if err != nil {
				return err
			}
			// add an entry to enable lookup based on FileId
			c.hlkeys[dr.FileId()] = dr
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
			"WorkspaceRoot buffer %s", key.String())

		if err := writeToChan(c, keyChan, "[hardlink table]", key,
			uint64(buf.Size()),
			quantumfs.ObjectTypeHardlink); err != nil {
			return err
		}

		hle = buf.AsHardlinkEntry()
	}
	return nil
}

func handleMultiBlockFile(c *Ctx, path string, ds quantumfs.DataStore,
	key quantumfs.ObjectKey, typ quantumfs.ObjectType,
	wf WalkFunc, keyChan chan<- *workerData) error {

	buf := simplebuffer.New(nil, key)
	if err := ds.Get(c.Qctx, key, buf); err != nil {
		return err
	}

	simplebuffer.AssertNonZeroBuf(buf,
		"MultiBlockFile buffer %s", key.String())

	if err := writeToChan(c, keyChan, path, key, uint64(buf.Size()),
		typ); err != nil {
		return err
	}

	mbf := buf.AsMultiBlockFile()
	keys := mbf.ListOfBlocks()
	for i, k := range keys {
		size := uint64(mbf.BlockSize())
		if i == len(keys)-1 {
			size = uint64(mbf.SizeOfLastBlock())
		}
		if err := writeToChan(c, keyChan, path, k, size,
			typ); err != nil {
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
		"VeryLargeFile buffer %s", key.String())

	if err := writeToChan(c, keyChan, path, key,
		uint64(buf.Size()), quantumfs.ObjectTypeVeryLargeFile); err != nil {
		return err
	}
	vlf := buf.AsVeryLargeFile()
	for part := 0; part < vlf.NumberOfParts(); part++ {
		if err := handleMultiBlockFile(c, path, ds, vlf.LargeFileKey(part),
			// ObjectTypeVeryLargeFile contains multiple
			// ObjectTypeLargeFile objects
			quantumfs.ObjectTypeLargeFile,
			wf, keyChan); err != nil && err != ErrSkipDirectory {

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
			"DirectoryEntry buffer %s", key.String())

		// When wf returns ErrSkipDirectory for a DirectoryEntry,
		// we can skip the DirectoryRecords in that DirectoryEntry
		if err := wf(c, path, key, uint64(buf.Size()),
			quantumfs.ObjectTypeDirectory); err != nil {
			if err == ErrSkipDirectory {
				return nil
			}
			return err
		}

		de := buf.AsDirectoryEntry()
		for i := 0; i < de.NumEntries(); i++ {
			if err := handleDirectoryRecord(c, path, ds, de.Entry(i), wf,
				keyChan); err != nil {

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
	dr *quantumfs.EncodedDirectoryRecord, wf WalkFunc,
	keyChan chan<- *workerData) error {

	// NOTE: some of the EncodedDirectoryRecord accesses eg: Filename, Size etc
	//       may not be meaningful for some EncodedDirectoryRecords. For example,
	//       Filename for EncodedDirectoryRecord in hardlinkRecord is
	//       meaningless
	//       Size for EncodedDirectoryRecord in special file is meaningless
	//
	//       This information is eventually sent to the walk handler
	//       function. Current use-cases don't need to filter such
	//       meaningless info, the default values work fine.
	fpath := filepath.Join(path, dr.Filename())

	if err := handleExtendedAttributes(c, fpath, ds, dr, keyChan); err != nil {
		return err
	}

	key := dr.ID()
	switch dr.Type() {
	case quantumfs.ObjectTypeMediumFile:
		// ObjectTypeMediumFile consists of ObjectTypeSmallFile
		return handleMultiBlockFile(c, fpath,
			ds, key, quantumfs.ObjectTypeSmallFile,
			wf, keyChan)
	case quantumfs.ObjectTypeLargeFile:
		// ObjectTypeLargeFile consists of ObjectTypeSmallFile
		return handleMultiBlockFile(c, fpath,
			ds, key, quantumfs.ObjectTypeSmallFile,
			wf, keyChan)
	case quantumfs.ObjectTypeVeryLargeFile:
		return handleVeryLargeFile(c, fpath,
			ds, key, wf, keyChan)
	case quantumfs.ObjectTypeDirectory:
		return handleDirectoryEntry(c, fpath,
			ds, key, wf, keyChan)
	case quantumfs.ObjectTypeHardlink:
		// This ObjectType will only be seen when looking at a
		// directoryRecord reached from directoryEntry and not
		// when walking from hardlinkEntry table hence use the
		// key from the hardlinkRecord.
		//
		// hardlinks cannot be made to directories so its safe
		// to just log the error if below checks fail and continue
		// walking other directory records in the caller.
		hldr, exists := c.hlkeys[dr.FileId()]

		if !exists {
			errStr := fmt.Sprintf("Key for hardlink Path: %s "+
				"FileId: %d missing in WSR hardlink info",
				fpath, dr.FileId())
			c.Qctx.Elog(qlog.LogTool, errStr)
			return nil
		} else if hldr.Type() == quantumfs.ObjectTypeHardlink {
			errStr := fmt.Sprintf("Hardlink object type found in"+
				"WSR hardlink info for path: %s fileID: %d", fpath,
				dr.FileId())
			c.Qctx.Elog(qlog.LogTool, errStr)
			return nil
		} else {
			// hldr could be of any of the supported ObjectTypes so
			// handle the directoryRecord accordingly
			return handleDirectoryRecord(c, fpath, ds, hldr, wf, keyChan)
		}
	case quantumfs.ObjectTypeSpecial:
		fallthrough
	case quantumfs.ObjectTypeSmallFile:
		fallthrough
	case quantumfs.ObjectTypeSymlink:
		fallthrough
	default:
		return writeToChan(c, keyChan, fpath, key, dr.Size(), dr.Type())
	}
}

func handleExtendedAttributes(c *Ctx, fpath string, ds quantumfs.DataStore,
	dr quantumfs.DirectoryRecord, keyChan chan<- *workerData) error {

	extKey := dr.ExtendedAttributes()
	if extKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		return nil
	}

	buf := simplebuffer.New(nil, extKey)
	if err := ds.Get(c.Qctx, extKey, buf); err != nil {
		return err
	}
	simplebuffer.AssertNonZeroBuf(buf,
		"Attributes List buffer %s", extKey.String())

	if err := writeToChan(c, keyChan, fpath, extKey, uint64(buf.Size()),
		quantumfs.ObjectTypeExtendedAttribute); err != nil {
		return err
	}

	attributeList := buf.AsExtendedAttributes()

	for i := 0; i < attributeList.NumAttributes(); i++ {
		_, key := attributeList.Attribute(i)

		buf := simplebuffer.New(nil, key)
		if err := ds.Get(c.Qctx, key, buf); err != nil {
			return err
		}
		simplebuffer.AssertNonZeroBuf(buf,
			"Attributes List buffer %s", key.String())

		// ObjectTypeExtendedAttribute is made up of
		// ObjectTypeSmallFile
		err := writeToChan(c, keyChan, fpath, key,
			uint64(buf.Size()), quantumfs.ObjectTypeSmallFile)
		if err != nil {
			return err
		}
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
		if err := wf(c, keyItem.path, keyItem.key, keyItem.size,
			keyItem.objType); err != nil && err != ErrSkipDirectory {
			return err
		}
	}
}

func writeToChan(c context.Context, keyChan chan<- *workerData, p string,
	k quantumfs.ObjectKey, s uint64, t quantumfs.ObjectType) error {

	select {
	case <-c.Done():
		return fmt.Errorf("Quitting writeToChan because at least one " +
			"goroutine failed with an error")
	case keyChan <- &workerData{path: p, key: k, size: s, objType: t}:
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
