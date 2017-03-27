// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "errors"
import "fmt"
import "os"
import "io/ioutil"
import "path/filepath"
import "sync"
import "syscall"
import "time"

import "golang.org/x/net/context"
import "golang.org/x/sync/errgroup"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

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
var mutex sync.Mutex
var topDirRecord quantumfs.DirectoryRecord
var rootTracker = true

func setupDirEntryTracker(path string, info os.FileInfo, recordCount int) {
	mutex.Lock()
	defer mutex.Unlock()
	dirEntryTrackers[path] = &dirEntryTracker{
		records: make([]quantumfs.DirectoryRecord, 0, recordCount),
		info:    info,
		root:    rootTracker,
	}
	rootTracker = false
}

func handleDirRecord(record quantumfs.DirectoryRecord, path string) error {
	// assert that mutex is held
	//fmt.Println("Handing dirRecord ", path)
	tracker, ok := dirEntryTrackers[path]
	if !ok {
		panic(fmt.Sprintf("DirEntry must exist for %s", path))
	}
	// must always have a vacant index
	tracker.records = append(tracker.records, record)
	if len(tracker.records) == cap(tracker.records) {
		//fmt.Println("Directory entry ", path, " is full")
		record, err := qwr.WriteDirectory(path, tracker.info, tracker.records, dataStore)
		if err != nil {
			return err
		}

		if tracker.root {
			topDirRecord = record
			return nil
		}

		err = handleDirRecord(record, filepath.Dir(path))
		if err != nil {
			return err
		}
	}
	return nil
}

func handlePathInfo(ctx context.Context, piChan <-chan *pathInfo) error {
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
			record, err = qwr.WriteFile(dataStore, msg.info, msg.path)
			if err != nil {
				return err
			}
		} else {
			// empty directory
			stat := msg.info.Sys().(*syscall.Stat_t)
			record = qwr.CreateNewDirRecord(msg.info.Name(), stat.Mode,
				uint32(stat.Rdev), 0,
				quantumfs.ObjectUid(stat.Uid, stat.Uid),
				quantumfs.ObjectGid(stat.Gid, stat.Gid),
				quantumfs.ObjectTypeDirectoryEntry,
				// retain time of the input directory
				quantumfs.NewTime(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)),
				quantumfs.NewTime(time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)),
				quantumfs.EmptyDirKey)
		}

		err = func() error {
			mutex.Lock()
			defer mutex.Unlock()
			return handleDirRecord(record, filepath.Dir(msg.path))
		}()
		if err != nil {
			return err
		}
	}
}

// base == "./somebase", path == "somebase"
// base == "somebase" , path == "somebase"
// base == "/somebase" , path == "/somebase"
// filepath.Clean() change 1 to 2
func relativePath(path string, base string) (string, error) {
	checkPath, relErr := filepath.Rel(filepath.Clean(base), path)
	//fmt.Println("RELPATH: ", filepath.Clean(base), path, checkPath)
	if relErr != nil {
		return "", relErr
	}
	if checkPath == "." {
		checkPath = "/"
	}
	return checkPath, nil
}

// when base is "./somebase" or "/somebase" or "somebase" then pathHandler is called with
//  path as "somebase" then path as "somebase/somedir" and so on
func pathHandler(ctx context.Context, piChan chan<- *pathInfo,
	path string, root string, info os.FileInfo, err error,
	exInfo *utils.ExcludeInfo) error {

	if err != nil {
		return err
	}

	checkPath, relErr := relativePath(path, root)
	if relErr != nil {
		return relErr
	}
	if exInfo != nil && exInfo.PathExcluded(checkPath) {
		//fmt.Println("Skipping ", path)
		if info.IsDir() {
			return filepath.SkipDir
		}

		return nil
	}
	//fmt.Println("Walking ", path, " ", checkPath)
	// a file's parent directory will always be seen before
	// the
	// check if path is a directory and its excluded
	// check if path is a file and its excluded

	// if path is a directory then find expected count of
	// DirectoryRecords for this path
	if info.IsDir() {
		dirEnts, dirErr := ioutil.ReadDir(path)
		if dirErr != nil {
			return dirErr
		}
		expectedDirRecords := len(dirEnts)
		if exInfo != nil {
			expectedDirRecords = exInfo.RecordsExcluded(checkPath, expectedDirRecords)
		}
		if expectedDirRecords > 0 {
			setupDirEntryTracker(path, info, expectedDirRecords)
			return nil
		}
	}

	// uploading a single regular file
	if checkPath == "/" && info.Mode().IsRegular() {
		parent := filepath.Dir(path)
		parentInfo, perr := os.Lstat(parent)
		if perr != nil {
			return perr
		}
		setupDirEntryTracker(parent, parentInfo, 1)
	}

	select {
	case <-ctx.Done():
		return errors.New("At least one worker has exited with error")
	case piChan <- &pathInfo{path: path, info: info}:
	}
	return nil
}

func upload(ws string, advance string, root string, exInfo *utils.ExcludeInfo,
	conc uint) error {

	group, groupCtx := errgroup.WithContext(context.Background())
	piChan := make(chan *pathInfo)

	// launch walker in same task group so that
	// error in walker will exit workers and vice versa

	// workers
	for i := uint(0); i < conc; i++ {
		group.Go(func() error {
			return handlePathInfo(groupCtx, piChan)
		})
	}
	// walker
	group.Go(func() error {
		//fmt.Println("=== Started handling dirs ===")
		err := filepath.Walk(root,
			func(path string, info os.FileInfo, err error) error {
				return pathHandler(groupCtx, piChan, path, root, info, err, exInfo)
			})
		close(piChan)
		return err
	})

	err := group.Wait()
	if err != nil {
		return err
	}

	//fmt.Println("=== Completed handling dirs ===")
	// assert topDirRecord != nil
	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(topDirRecord.ID(), dataStore)
	if wsrErr != nil {
		return wsrErr
	}

	return qwr.CreateWorkspace(wsDB, ws, advance, wsrKey)
}
