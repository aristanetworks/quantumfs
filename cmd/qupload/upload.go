// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "errors"
import "fmt"
import "os"
import "io/ioutil"
import "path/filepath"
import "syscall"
import "time"

import "golang.org/x/net/context"
import "golang.org/x/sync/errgroup"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/utils"

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

var dirEntryTrackers = make(map[string]*dirEntryTracker)
var dirStateMutex utils.DeferableMutex
var topDirRecord quantumfs.DirectoryRecord
var rootTracker = true

func setupDirEntryTracker(path string, info os.FileInfo, recordCount int) {
	defer dirStateMutex.Lock().Unlock()
	dirEntryTrackers[path] = &dirEntryTracker{
		records: make([]quantumfs.DirectoryRecord, 0, recordCount),
		info:    info,
		root:    rootTracker,
	}
	rootTracker = false
}

func handleDirRecord(record quantumfs.DirectoryRecord, path string) error {
	var err error
	defer dirStateMutex.Lock().Unlock()

	for {
		tracker, ok := dirEntryTrackers[path]
		if !ok {
			panic(fmt.Sprintf("BUG: Directory state tracker must "+
				"exist for %q", path))
		}

		tracker.records = append(tracker.records, record)
		if len(tracker.records) != cap(tracker.records) {
			return nil
		}
		record, err = qwr.WriteDirectory(path, tracker.info,
			tracker.records, dataStore)
		if err != nil {
			return err
		}

		if tracker.root {
			topDirRecord = record
			return nil
		}
		// we flushed current dir which could be the last
		// subdir for parent
		path = filepath.Dir(path)
	}
}

func pathWorker(ctx context.Context, piChan <-chan *pathInfo) error {
	var record quantumfs.DirectoryRecord
	var err error
	var msg *pathInfo

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg = <-piChan:
			if msg == nil {
				return nil
			}
		}

		if !msg.info.IsDir() {
			// WriteFile() will detect the file type based on
			// stat information and setup appropriate data
			// and metadata for the file in storage
			record, err = qwr.WriteFile(dataStore,
				msg.info, msg.path)
			if err != nil {
				return err
			}
		} else {
			// walker walks non-empty directories to generate
			// worker handles empty directory
			stat := msg.info.Sys().(*syscall.Stat_t)
			record = qwr.CreateNewDirRecord(msg.info.Name(),
				stat.Mode, uint32(stat.Rdev), 0,
				quantumfs.ObjectUid(stat.Uid, stat.Uid),
				quantumfs.ObjectGid(stat.Gid, stat.Gid),
				quantumfs.ObjectTypeDirectoryEntry,
				// retain time of the input directory
				quantumfs.NewTime(time.Unix(stat.Mtim.Sec,
					stat.Mtim.Nsec)),
				quantumfs.NewTime(time.Unix(stat.Ctim.Sec,
					stat.Ctim.Nsec)),
				quantumfs.EmptyDirKey)
		}

		err = handleDirRecord(record, filepath.Dir(msg.path))
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

func pathWalker(ctx context.Context, piChan chan<- *pathInfo,
	path string, root string, info os.FileInfo, err error,
	exInfo *ExcludeInfo) error {

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
	if exInfo != nil && exInfo.PathExcluded(checkPath) {
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
		if exInfo != nil {
			expectedDirRecords = exInfo.RecordsExcluded(checkPath,
				expectedDirRecords)
		}
		// Empty directory is like a file with no content
		// whose parent directory state tracking has already been
		// setup. Hence empty directory is handled by worker to maintain
		// separation of concerns between walker and worker
		if expectedDirRecords > 0 {
			setupDirEntryTracker(path, info, expectedDirRecords)
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
		setupDirEntryTracker(parent, parentInfo, 1)
	}

	// send pathInfo to workers
	select {
	case <-ctx.Done():
		return errors.New("Exiting walker since qt least one worker " +
			"has exited with error")
	case piChan <- &pathInfo{path: path, info: info}:
	}
	return nil
}

func upload(ws string, advance string, root string, exInfo *ExcludeInfo,
	conc uint) error {

	// launch walker in same task group so that
	// error in walker will exit workers and vice versa
	group, groupCtx := errgroup.WithContext(context.Background())
	piChan := make(chan *pathInfo)

	// workers
	for i := uint(0); i < conc; i++ {
		group.Go(func() error {
			return pathWorker(groupCtx, piChan)
		})
	}
	// walker
	group.Go(func() error {
		err := filepath.Walk(root,
			func(path string, info os.FileInfo, err error) error {
				return pathWalker(groupCtx, piChan, path,
					root, info, err, exInfo)
			})
		close(piChan)
		return err
	})

	err := group.Wait()
	if err != nil {
		return err
	}

	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(topDirRecord.ID(), dataStore)
	if wsrErr != nil {
		return wsrErr
	}
	return qwr.CreateWorkspace(wsDB, ws, advance, wsrKey)
}
