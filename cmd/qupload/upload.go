// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	exs "github.com/aristanetworks/quantumfs/utils/excludespec"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// Design notes about qupload parallelism:
//
// A directory is uploaded after all its children have been uploaded. A state
// is maintained for each directory which tracks how many of its children have
// been uploaded so far. The state helps in deciding when to upload the
// directory.
//
// Walker walks the input directory hierarchy and hands-off path information
// to workers for the upload. Worker uploads the files or directories.
// There is 1 walker and "concurrency" number of workers.

// Walker sets up the per-directory state. The state is used by workers to
// find out when a directory can be uploaded. When a worker finishes uploading
// a file, it checks the file's parent directory's state to see if the
// directory is ready to be uploaded. If not ready then the worker goes off to
// upload another file. If the directory is ready to be uploaded then the
// worker uploads it and checks the directory's parent for upload readiness.
// Workers are not specific to a directory. Since all workers listen on the
// same channel, looking for path information, its possible that different
// workers upload different files within the same directory.
//
// Since walker uses filepath.Walk, the files and subdirectories within
// a directory are traversed in a lexical order. When a subdirectory is found,
// its descended into and the walk continues.

type pathInfo struct {
	path string
	info os.FileInfo
}

type dirEntryTracker struct {
	records []quantumfs.DirectoryRecord
	info    os.FileInfo
	root    bool
}

type Uploader struct {
	dataStore quantumfs.DataStore
	wsDB      quantumfs.WorkspaceDB
	exInfo    *exs.ExcludeInfo

	topDirID quantumfs.ObjectKey

	dirEntryTrackers map[string]*dirEntryTracker
	dirStateMutex    utils.DeferableMutex

	hardlinks *qwr.Hardlinks

	dataBytesWritten     uint64
	metadataBytesWritten uint64
}

func NewUploader() Uploader {
	return Uploader{
		dirEntryTrackers: make(map[string]*dirEntryTracker),
		hardlinks:        qwr.NewHardlinks(),
	}
}

func (up *Uploader) setupDirEntryTracker(path string, root_ string, info os.FileInfo,
	recordCount int) {

	defer up.dirStateMutex.Lock().Unlock()
	up.dirEntryTrackers[path] = &dirEntryTracker{
		records: make([]quantumfs.DirectoryRecord, 0, recordCount),
		info:    info,
		root:    path == root_,
	}
}

func (up *Uploader) dumpUploadState() {
	// exclude info
	fmt.Println(up.exInfo)
	// dump paths which have pending writes
	for path, tracker := range up.dirEntryTrackers {
		if len(tracker.records) != cap(tracker.records) {
			fmt.Printf("%q (root: %v) is pending writes\n",
				path, tracker.root)
			fmt.Printf("  len: %d cap: %d\n",
				len(tracker.records),
				cap(tracker.records))
			for _, rec := range tracker.records {
				fmt.Printf("   %s\n", rec.Filename())
			}
		}
	}
}

func (up *Uploader) handleDirRecord(qctx *quantumfs.Ctx,
	record quantumfs.DirectoryRecord, path string) (err error) {

	defer up.dirStateMutex.Lock().Unlock()

	for {
		tracker, ok := up.dirEntryTrackers[path]
		if !ok {
			up.dumpUploadState()
			panic(fmt.Sprintf("PANIC: Directory state tracker must "+
				"exist for %q", path))
		}

		tracker.records = append(tracker.records, record)
		if len(tracker.records) != cap(tracker.records) {
			return nil
		}
		qctx.Vlog(qlog.LogTool, "Writing %s", path)
		var written uint64
		record, written, err = qwr.WriteDirectory(qctx, path, tracker.info,
			tracker.records, up.dataStore)
		if err != nil {
			return err
		}
		atomic.AddUint64(&up.metadataBytesWritten, written)

		if tracker.root {
			up.topDirID = record.ID()
			return nil
		}
		// we flushed current dir which could be the last
		// subdir for parent
		path = filepath.Dir(path)
	}
}

func (up *Uploader) processPath(c *Ctx,
	msg *pathInfo) (rtn quantumfs.DirectoryRecord, dataWritten uint64,
	metadataWritten uint64, err error) {

	if !msg.info.IsDir() {
		// WriteFile() will detect the file type based on
		// stat information and setup appropriate data
		// and metadata for the file in storage
		c.Vlog("Writing %s", msg.path)
		return qwr.WriteFile(c.Qctx, up.dataStore, msg.info,
			msg.path, up.hardlinks)
	} else {
		// walker walks non-empty directories to generate
		// worker handles empty directory
		stat := msg.info.Sys().(*syscall.Stat_t)
		record := qwr.CreateNewDirRecord(msg.info.Name(),
			stat.Mode, uint32(stat.Rdev), 0,
			quantumfs.ObjectUid(stat.Uid, stat.Uid),
			quantumfs.ObjectGid(stat.Gid, stat.Gid),
			quantumfs.ObjectTypeDirectory,
			// retain time of the input directory
			quantumfs.NewTime(time.Unix(stat.Mtim.Sec,
				stat.Mtim.Nsec)),
			quantumfs.NewTime(time.Unix(stat.Ctim.Sec,
				stat.Ctim.Nsec)),
			quantumfs.EmptyDirKey)

		return record, 0, 0, nil
	}
}

func (up *Uploader) pathWorker(c *Ctx, piChan <-chan *pathInfo) error {

	var msg *pathInfo

	for {
		select {
		case <-c.eCtx.Done():
			return nil
		case msg = <-piChan:
			if msg == nil {
				return nil
			}
		}

		record, dataWritten, metadataWritten, err := up.processPath(c, msg)
		if err != nil {
			return err
		}
		atomic.AddUint64(&up.dataBytesWritten, dataWritten)
		atomic.AddUint64(&up.metadataBytesWritten, metadataWritten)

		err = up.handleDirRecord(c.Qctx, record, filepath.Dir(msg.path))
		if err != nil {
			return err
		}
	}
}

// if base == "a/b/c" and path "c/d" then
// relativePath is "d"
func relativePath(path string, base string) (string, error) {
	checkPath, relErr := filepath.Rel(filepath.Clean(base), path)
	if relErr != nil {
		return "", fmt.Errorf("relativePath: %q %q error: %v",
			path, base, relErr)
	}
	if checkPath == "." {
		checkPath = "/"
	}
	return checkPath, nil
}

func (up *Uploader) pathWalker(c *Ctx, piChan chan<- *pathInfo,
	path string, root string, info os.FileInfo, err error) error {

	// when basedir is "./somebase" or "/somebase" or "somebase" then
	// pathWalker is called with path as "somebase" then path as
	// "somebase/somedir" and so on
	if err != nil {
		return err
	}

	// exclude files and directories per exclude
	// file spec
	checkPath, relErr := relativePath(path, root)
	if relErr != nil {
		return relErr
	}
	if up.exInfo != nil && up.exInfo.PathExcluded(checkPath) {
		if info.IsDir() {
			return filepath.SkipDir
		}
		return nil
	}

	// For directories, establish proper state
	// tracking while accounting for excluded files
	// and subdirs in parent dir so that its state
	// tracking is terminated appropriately.
	if info.IsDir() {
		dirEnts, dirErr := ioutil.ReadDir(path)
		if dirErr != nil {
			return fmt.Errorf("pathWalker ReadDir error: %v", dirErr)
		}
		expectedDirRecords := len(dirEnts)
		if up.exInfo != nil {
			expectedDirRecords = up.exInfo.RecordCount(checkPath,
				expectedDirRecords)
		}
		// Empty directory is like a file with no content
		// whose parent directory state tracking has already been
		// setup. Hence empty directory is handled by worker to maintain
		// separation of concerns between walker and worker
		if expectedDirRecords > 0 || path == root {
			up.setupDirEntryTracker(path, root, info, expectedDirRecords)
			return nil
		}
	}

	// This section is an optimization for scenario where qupload
	// is invoked to upload a single file. The optimization is that
	// one doesn't need to specify an exclude file to exlude rest of the
	// content in that directory. The qupload invocation would be
	// qupload -basedir somedir .... filename
	// in such case the pathWalker never gets a directory as input and
	// so we need to special case it for setting up the directory
	// state tracker
	// This section isn't used when multiple files within the basedir
	// are to be uploaded
	if checkPath == "/" && info.Mode().IsRegular() {
		parent := filepath.Dir(path)
		parentInfo, perr := os.Lstat(parent)
		if perr != nil {
			return fmt.Errorf("pathWalker parent %q error: %v",
				parent, perr)
		}
		// parentInfo helps in setting up the directory entry
		// under workspace root
		up.setupDirEntryTracker(parent, root, parentInfo, 1)
	}

	// send pathInfo to workers
	select {
	case <-c.eCtx.Done():
		return errors.New("Exiting walker since qt least one worker " +
			"has exited with error")
	case piChan <- &pathInfo{path: path, info: info}:
	}
	return nil
}

func (up *Uploader) upload(c *Ctx, cli *params,
	relpath string) (wsrKey quantumfs.ObjectKey, rtn error) {

	ws := cli.ws
	aliasWS := cli.alias
	root := filepath.Join(cli.baseDir, relpath)
	conc := cli.conc

	start := time.Now()
	// launch walker in same task group so that
	// error in walker will exit workers and vice versa
	var group *errgroup.Group
	group, c.eCtx = errgroup.WithContext(context.Background())
	piChan := make(chan *pathInfo)

	// workers
	for i := uint(0); i < conc; i++ {
		group.Go(func() error {
			return up.pathWorker(c, piChan)
		})
	}
	// walker
	group.Go(func() error {
		err := filepath.Walk(root,
			func(path string, info os.FileInfo, err error) error {
				return up.pathWalker(c, piChan, path,
					root, info, err)
			})
		close(piChan)
		return err
	})

	err := group.Wait()
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}

	var emptyKey quantumfs.ObjectKey
	if up.topDirID.IsEqualTo(emptyKey) {
		// check if the root directory is empty
		if up.exInfo != nil && up.exInfo.RecordCount(root, 0) != 0 {
			up.dumpUploadState()
			panic("PANIC: workspace root dir not written yet but all " +
				"writes to workspace completed." +
				"Use debug dump to diagnose.")
		}

		c.Wlog("Empty workspace root detected.")
		up.topDirID = quantumfs.EmptyDirKey
	}

	wsrKey, written, wsrErr := qwr.WriteWorkspaceRoot(c.Qctx, up.topDirID,
		up.dataStore, up.hardlinks)
	if wsrErr != nil {
		return quantumfs.ObjectKey{}, wsrErr
	}
	atomic.AddUint64(&up.metadataBytesWritten, written)

	err = uploadCompleted(c.Qctx, up.wsDB, ws, aliasWS, wsrKey)
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}

	fmt.Printf("\nUpload completed. Total: %d bytes "+
		"(Data:%d(%d%%) Metadata:%d(%d%%)) in %.0f secs to %s\n",
		up.dataBytesWritten+up.metadataBytesWritten,
		up.dataBytesWritten,
		(up.dataBytesWritten*100)/
			(up.dataBytesWritten+up.metadataBytesWritten),
		up.metadataBytesWritten,
		(up.metadataBytesWritten*100)/
			(up.dataBytesWritten+up.metadataBytesWritten),
		time.Since(start).Seconds(), ws)
	return wsrKey, nil
}

// Uploads to existing workspaces should be supported.
//
// WSDB_WORKSPACE_EXISTS error in Branch API can happen if the workspace got
// created during upload, workspace already existed (use of -wsforce option) etc.
// The error should be ignored in case -wsforce option is being used.
// The error can be ignored if the workspace was created during upload since
// the upload _may_ fail during advance. The advance API may return
// WSDB_OUT_OF_DATE error and the upload will be failed.
func branchThenAdvance(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB,
	wsname string,
	newKey quantumfs.ObjectKey) error {

	wsParts := strings.Split(wsname, "/")
	err := wsdb.BranchWorkspace(qctx,
		quantumfs.NullSpaceName,
		quantumfs.NullSpaceName,
		quantumfs.NullSpaceName,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		if wsdbErrCode, ok := err.(quantumfs.WorkspaceDbErr); ok {
			if wsdbErrCode.Code != quantumfs.WSDB_WORKSPACE_EXISTS {
				// see note above on why we can ignore
				// this error
				return err
			}
		}
	}

	var curKey quantumfs.ObjectKey
	curKey, nonce, err := wsdb.Workspace(qctx, wsParts[0], wsParts[1],
		wsParts[2])
	if err != nil {
		return err
	}
	_, err = wsdb.AdvanceWorkspace(qctx, wsParts[0], wsParts[1], wsParts[2],
		nonce, curKey, newKey)
	if err != nil {
		return err
	}

	return nil
}

func uploadCompleted(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB, ws string,
	aliasWS string, newWsrKey quantumfs.ObjectKey) error {

	err := branchThenAdvance(qctx, wsdb, ws, newWsrKey)
	if err != nil {
		return err
	}

	if aliasWS != "" {
		err := branchThenAdvance(qctx, wsdb, aliasWS, newWsrKey)
		if err != nil {
			return err
		}
	}
	err = SetImmutable(qctx, wsdb, ws)
	if err != nil {
		return err
	}

	return nil
}

func (up *Uploader) byPass(c *Ctx, cli *params) error {

	ws := cli.ws
	aliasWS := cli.alias
	referenceWS := cli.referenceWS
	refWSParts := strings.Split(referenceWS, "/")

	refKey, _, err := up.wsDB.Workspace(c.Qctx,
		refWSParts[0], refWSParts[1], refWSParts[2])
	if err != nil {
		return err
	}

	err = uploadCompleted(c.Qctx, up.wsDB, ws, aliasWS, refKey)
	if err != nil {
		return err
	}
	fmt.Printf("ByPass Completed: %v -> %v\n", ws, referenceWS)
	return nil
}

func SetImmutable(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB,
	wsname string) error {

	wsParts := strings.Split(wsname, "/")
	return wsdb.SetWorkspaceImmutable(qctx, wsParts[0], wsParts[1],
		wsParts[2])
}
