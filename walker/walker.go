// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
	"golang.org/x/sync/errgroup"
)

// ErrSkipHierarchy is a special error from WalkFunc to indicate that
// the walker should skip the current workspace hiearchy and
// continue with rest of the workspace walk. This error also
// leaves it upto the walker library to decide whether it wishes
// to exit or continue walking.
// This error will not be treated as an error during workspace walk.
var ErrSkipHierarchy = errors.New("skip this hierarchy")

// WalkFunc is the type of the function called for each data block under the
// Workspace. Every error encountered by walker library can be filtered by
// WalkFunc. So walker library does not log any errors, instead it forwards all
// errors to the walkFunc. Hence the right place to harvest errors is the
// WalkFunc. Hence even if Walk returns nil error, it could still mean that there
// were errors during the walk. If WalkFunc returns any error, except
// ErrSkipDirectory, then the workspace walk is stopped. So depending on the error
// handling behaviour of WalkFunc, Walk API can be used to do a fail-fast (abort walk
// on first error) or a best-effort walk (continue walk amidst errors).
//
// When err argument is non-nil, size is invalid.
//
// When err argument is non-nil, path may be empty. When path is empty, key, size
// and objType are invalid.
type WalkFunc func(ctx *Ctx, path string, key quantumfs.ObjectKey,
	size uint64, objType quantumfs.ObjectType,
	err error) error

// filterErrByWalkFunc is used to forward the walker library
// encountered errors to walkFunc. Use of this function
// makes the error forwarding more clear than using the
// walkFunc directly.
func filterErrByWalkFunc(c *Ctx, path string, key quantumfs.ObjectKey,
	objTyp quantumfs.ObjectType, err error) error {

	// size is invalid, see note above.
	return c.wf(c, path, key, 0, objTyp, err)
}

// walkDsGet enables mocking datastore Get in test routines.
type walkDsGet func(cq *quantumfs.Ctx, path string,
	key quantumfs.ObjectKey, typ quantumfs.ObjectType,
	buf quantumfs.Buffer) error

// Ctx maintains context for the walker library.
type Ctx struct {
	context.Context
	Qctx   *quantumfs.Ctx
	rootID quantumfs.ObjectKey

	hlkeys map[quantumfs.FileId]*quantumfs.EncodedDirectoryRecord
	wf     WalkFunc
	group  *errgroup.Group
	dsGet  walkDsGet
}

type workerData struct {
	path    string
	key     quantumfs.ObjectKey
	size    uint64
	objType quantumfs.ObjectType
}

const panicErrFmt = "PANIC %s\n%v"

func panicToError(exception interface{}) error {
	var result string
	switch exception.(type) {
	default:
		result = fmt.Sprintf("Unknown panic type: %v", exception)
	case string:
		result = exception.(string)
	case error:
		result = exception.(error).Error()
	}

	trace := utils.BytesToString(debug.Stack())

	return fmt.Errorf(panicErrFmt, result, trace)
}

const walkerErrLog = "desc: %s key: %s err: %s"

// aggregateDsGetter returns a datastore getter function that
// first checks in ConstantStore and then falls back to the
// provied datastore getter.
func aggregateDsGetter(dsGet walkDsGet) walkDsGet {
	return func(cq *quantumfs.Ctx, path string,
		key quantumfs.ObjectKey, typ quantumfs.ObjectType,
		buf quantumfs.Buffer) error {

		if err := quantumfs.ConstantStore.Get(cq, key, buf); err != nil {
			return dsGet(cq, path, key, typ, buf)
		}
		return nil
	}
}

// dsErrFilter forwards errors from dsGet invocations to
// walkFunc.
func dsErrFilter(c *Ctx, dsGet walkDsGet) walkDsGet {
	return func(cq *quantumfs.Ctx, path string,
		key quantumfs.ObjectKey, typ quantumfs.ObjectType,
		buf quantumfs.Buffer) error {

		err := dsGet(cq, path, key, typ, buf)
		if err != nil {
			err = filterErrByWalkFunc(c, path, key, typ, err)
		}
		return err
	}
}

func newContext(cq *quantumfs.Ctx, dsGet walkDsGet,
	rootID quantumfs.ObjectKey, wf WalkFunc) *Ctx {

	group, groupCtx := errgroup.WithContext(context.Background())
	return &Ctx{
		Context: groupCtx,
		Qctx:    cq,
		rootID:  rootID,
		hlkeys: make(
			map[quantumfs.FileId]*quantumfs.EncodedDirectoryRecord),
		wf:    wf,
		group: group,
		dsGet: dsGet,
	}
}

// Walk the workspace hierarchy
func Walk(cq *quantumfs.Ctx, ds quantumfs.DataStore, rootID quantumfs.ObjectKey,
	wf WalkFunc) error {

	getter := func(cq *quantumfs.Ctx, path string,
		key quantumfs.ObjectKey, typ quantumfs.ObjectType,
		buf quantumfs.Buffer) error {

		return ds.Get(cq, key, buf)
	}

	c := newContext(cq, getter, rootID, wf)

	// since test routines directly call walk()
	// ensure that Walk() doesn't add anything
	// else here.
	return walk(c)
}

// walk is the core walker routine which accepts a context.
// This enables test routines to setup appropriate context
// to simulate different conditions while keeping the API
// simple.
func walk(c *Ctx) error {
	// we use the aggregated data store throughout walker
	// but we don't overwrite the c.dsGet since thats the
	// caller provided dsGet
	adsGet := aggregateDsGetter(c.dsGet)
	// a dsGet wrapper which first queries from aggregate datastore and then the
	// real datastore. Any error during dsGet is filtered using walkFunc.
	adsGetEF := dsErrFilter(c, adsGet)

	var err error
	buf := simplebuffer.New(nil, c.rootID)
	if err = adsGetEF(c.Qctx, "wsr", c.rootID,
		quantumfs.ObjectTypeWorkspaceRoot, buf); err != nil {

		return err
	}
	simplebuffer.AssertNonZeroBuf(buf,
		"WorkspaceRoot buffer %s", c.rootID.String())

	wsr := buf.AsWorkspaceRoot()
	//===============================================
	// NOTE: currently we only use base layer key
	// rootID is ObjectKey of type KeyTypeMetadata which refers to
	// WorkspaceRoot. The BaseLayer() is ObjectKey of type
	// KeyTypeMetadata which refers to an ObjectType of
	// DirectoryEntry

	keyChan := make(chan *workerData, 100)

	// Start Workers
	conc := runtime.GOMAXPROCS(-1)
	for i := 0; i < conc; i++ {

		c.group.Go(func() (err error) {
			return worker(c, keyChan)
		})
	}

	c.group.Go(func() (err error) {
		defer close(keyChan)
		defer func() {
			if exception := recover(); exception != nil {
				// If walker goroutine panics then irrespective
				// of walkFunc error filtering, we'll end the
				// workspace walk.
				err = panicToError(exception)
				filterErrByWalkFunc(c, "", quantumfs.ObjectKey{},
					quantumfs.ObjectTypeSmallFile, err)
			}
		}()

		// WSR
		if err = writeToChan(c, keyChan, "[rootId]", c.rootID,
			uint64(buf.Size()),
			quantumfs.ObjectTypeWorkspaceRoot); err != nil {

			return err
		}

		if err = handleHardLinks(c, adsGetEF, wsr.HardlinkEntry(),
			keyChan); err != nil {

			return err
		}

		// all the hardlinks in this workspace must be walked
		// prior to starting the walk of the root directory to
		// enable lookup for fileID in directoryRecord of the
		// path which represents the hardlink

		if err = handleDirectoryEntry(c, "/", adsGetEF, wsr.BaseLayer(),
			keyChan); err != nil {

			return err
		}

		return nil
	})

	return c.group.Wait()
}

func handleHardLinks(c *Ctx, adsGetEF walkDsGet,
	hle quantumfs.HardlinkEntry, keyChan chan<- *workerData) error {

	for {
		// Go through all records in this entry.
		for idx := 0; idx < hle.NumEntries(); idx++ {

			hlr := hle.Entry(idx)
			dr := hlr.Record()
			linkPath := dr.Filename()
			err := handleDirectoryRecord(c, linkPath, adsGetEF, dr,
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
		if err := adsGetEF(c.Qctx, "[hardlink table]",
			key, quantumfs.ObjectTypeHardlink, buf); err != nil {

			if err == ErrSkipHierarchy {
				return nil
			}
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

func handleMultiBlockFile(c *Ctx, path string, dsGet walkDsGet,
	key quantumfs.ObjectKey, typ quantumfs.ObjectType,
	keyChan chan<- *workerData) error {

	buf := simplebuffer.New(nil, key)
	if err := dsGet(c.Qctx, path, key, typ, buf); err != nil {
		if err == ErrSkipHierarchy {
			return nil
		}
		return err
	}

	simplebuffer.AssertNonZeroBuf(buf,
		"MultiBlockFile buffer %s", key.String())

	// indicate metadata block's ObjectType
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
		// multi-block files are always made up of
		// small files
		if err := writeToChan(c, keyChan, path, k, size,
			quantumfs.ObjectTypeSmallFile); err != nil {
			return err
		}
	}

	return nil
}

func handleVeryLargeFile(c *Ctx, path string, dsGet walkDsGet,
	key quantumfs.ObjectKey, keyChan chan<- *workerData) error {

	buf := simplebuffer.New(nil, key)
	if err := dsGet(c.Qctx, path, key,
		quantumfs.ObjectTypeVeryLargeFile, buf); err != nil {

		if err == ErrSkipHierarchy {
			return nil
		}
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
		// ObjectTypeVeryLargeFile contains multiple
		// ObjectTypeLargeFile objects
		if err := handleMultiBlockFile(c, path, dsGet,
			vlf.LargeFileKey(part), quantumfs.ObjectTypeLargeFile,
			keyChan); err != nil && err != ErrSkipHierarchy {

			return err
		}
	}

	return nil
}

func handleDirectoryEntry(c *Ctx, path string, adsGetEF walkDsGet,
	key quantumfs.ObjectKey, keyChan chan<- *workerData) error {

	for {
		buf := simplebuffer.New(nil, key)
		if err := adsGetEF(c.Qctx, path, key,
			quantumfs.ObjectTypeDirectory, buf); err != nil {

			if err == ErrSkipHierarchy {
				return nil
			}
			return err
		}

		simplebuffer.AssertNonZeroBuf(buf,
			"DirectoryEntry buffer %s", key.String())

		// When wf returns ErrSkipHierarchy for a DirectoryEntry,
		// we can skip the DirectoryRecords in that DirectoryEntry
		if err := c.wf(c, path, key, uint64(buf.Size()),
			quantumfs.ObjectTypeDirectory, nil); err != nil {

			if err == ErrSkipHierarchy {
				return nil
			}
			return err
		}

		de := buf.AsDirectoryEntry()
		for i := 0; i < de.NumEntries(); i++ {
			if err := handleDirectoryRecord(c, path, adsGetEF,
				de.Entry(i), keyChan); err != nil {

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

func handleDirectoryRecord(c *Ctx, path string, adsGetEF walkDsGet,
	dr *quantumfs.EncodedDirectoryRecord, keyChan chan<- *workerData) error {

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

	if err := handleExtendedAttributes(c, fpath, adsGetEF,
		dr, keyChan); err != nil {

		return err
	}

	key := dr.ID()
	switch dr.Type() {
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		return handleMultiBlockFile(c, fpath,
			adsGetEF, key, dr.Type(), keyChan)
	case quantumfs.ObjectTypeVeryLargeFile:
		return handleVeryLargeFile(c, fpath, adsGetEF, key, keyChan)
	case quantumfs.ObjectTypeDirectory:
		return handleDirectoryEntry(c, fpath, adsGetEF, key, keyChan)
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
			err := fmt.Errorf("%s", errStr)
			err = filterErrByWalkFunc(c, fpath, key,
				quantumfs.ObjectTypeHardlink, err)
			if err == ErrSkipHierarchy {
				return nil
			}
			return err
		} else if hldr.Type() == quantumfs.ObjectTypeHardlink {
			errStr := fmt.Sprintf("Hardlink object type found in"+
				"WSR hardlink info for path: %s fileID: %d", fpath,
				dr.FileId())
			err := fmt.Errorf("%s", errStr)
			err = filterErrByWalkFunc(c, fpath, key,
				quantumfs.ObjectTypeHardlink, err)
			if err == ErrSkipHierarchy {
				return nil
			}
			return err
		} else {
			// hldr could be of any of the supported ObjectTypes so
			// handle the directoryRecord accordingly
			return handleDirectoryRecord(c, fpath, adsGetEF, hldr,
				keyChan)
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

func handleExtendedAttributes(c *Ctx, fpath string, adsGetEF walkDsGet,
	dr quantumfs.DirectoryRecord, keyChan chan<- *workerData) error {

	extKey := dr.ExtendedAttributes()
	if extKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		return nil
	}

	buf := simplebuffer.New(nil, extKey)
	if err := adsGetEF(c.Qctx, fpath, extKey,
		quantumfs.ObjectTypeExtendedAttribute, buf); err != nil {

		if err == ErrSkipHierarchy {
			return nil
		}
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
		if err := adsGetEF(c.Qctx, fpath, key,
			quantumfs.ObjectTypeExtendedAttribute, buf); err != nil {

			if err == ErrSkipHierarchy {
				return nil
			}
			return err
		}
		simplebuffer.AssertNonZeroBuf(buf,
			"Attributes List buffer %s", key.String())

		// ObjectTypeExtendedAttribute is made up of
		// ObjectTypeSmallFile
		if err := writeToChan(c, keyChan, fpath, key,
			uint64(buf.Size()),
			quantumfs.ObjectTypeSmallFile); err != nil {

			return err
		}
	}
	return nil
}

// _worker handles multiple items from keyChan and converts any panics into errors
// during processing.
func _worker(c *Ctx, keyChan <-chan *workerData) (err error) {
	defer func() {
		if exception := recover(); exception != nil {
			// The panic in walkFunc is converted to error and the
			// filtered error is returned.
			err = panicToError(exception)
			err = filterErrByWalkFunc(c, "", quantumfs.ObjectKey{},
				quantumfs.ObjectTypeSmallFile, err)
		}
	}()

	var keyItem *workerData
	for {
		select {
		case <-c.Done():
			err = fmt.Errorf("Quitting worker because at least one " +
				"goroutine failed with an error")
			return
		case keyItem = <-keyChan:
			if keyItem == nil {
				err = nil
				return
			}
		}

		if err = c.wf(c, keyItem.path, keyItem.key, keyItem.size,
			keyItem.objType, nil); err != nil &&
			err != ErrSkipHierarchy {
			return
		}
	}
}

// worker knows when to terminate the worker goroutine.
func worker(c *Ctx, keyChan <-chan *workerData) (err error) {
	for {
		err = _worker(c, keyChan)
		// close the worker is an err different than Errskiphierarchy
		// is seen or when the walker goroutine has terminated.
		if err == nil || err != ErrSkipHierarchy {
			return
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
