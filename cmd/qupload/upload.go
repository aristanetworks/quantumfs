// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "fmt"
import "os"
import "io/ioutil"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

import "golang.org/x/net/context"
import "golang.org/x/sync/errgroup"

// by default exclusion list is not checked
var enableExclChecks = false

func handleDirContents(ctx context.Context,
	childWg *errgroup.Group, childDirRecChan chan<- *quantumfs.DirectoryRecord,
	base string, relpath string,
	dirEnts []os.FileInfo,
	ds quantumfs.DataStore) {

	// recurse over dirs, ignore files
	for _, dirEnt := range dirEnts {
		if !dirEnt.IsDir() {
			continue
		}
		name := filepath.Join(relpath, dirEnt.Name())
		if enableExclChecks && utils.IsPathExcluded(name) {
			continue
		}

		childWg.Go(func() error {
			return handleDir(ctx, childDirRecChan, base, name, ds)
		})
	}

	// handle all files within cur dir
	childWg.Go(func() error {
		for _, dirEnt := range dirEnts {
			if dirEnt.IsDir() {
				continue
			}

			name := filepath.Join(relpath, dirEnt.Name())
			if enableExclChecks && utils.IsPathExcluded(name) {
				continue
			}
			record, ferr := qwr.WriteFile(ds, dirEnt, filepath.Join(base, name))
			if ferr != nil {
				return ferr
			}

			//fmt.Printf("File:%s Records: %v\n", dirEnt.Name(), record)
			childDirRecChan <- record
		}

		return nil
	})
}

func handleDir(ctx context.Context, dirRecordChan chan<- *quantumfs.DirectoryRecord,
	base string, relpath string,
	ds quantumfs.DataStore) error {

	var curDirRecords []*quantumfs.DirectoryRecord

	// create a child context
	childWg, childCtx := errgroup.WithContext(ctx)
	// a directory record channel for go-routines
	// operating in the child context
	childDirRecChan := make(chan *quantumfs.DirectoryRecord)

	// this relpath is included, check if it's contents
	// are excluded
	if !enableExclChecks || !utils.IsPathExcluded(relpath+"/") {
		curDirPath := filepath.Join(base, relpath)
		dirEnts, err := ioutil.ReadDir(curDirPath)
		if err != nil {
			close(childDirRecChan)
			return fmt.Errorf("Reading %s failed %s\n", curDirPath, err)
		}
		handleDirContents(childCtx, childWg, childDirRecChan, base, relpath, dirEnts, ds)
	}

	// when all the workers in childWg have exited
	// then close the childDirRecChan
	go func() {
		childWg.Wait()
		close(childDirRecChan)
	}()

	// collect DirectoryRecords over the channel
	for dirRecord := range childDirRecChan {
		curDirRecords = append(curDirRecords, dirRecord)
	}

	// once childDirRecChan closes check if there
	// was an error
	if err := childWg.Wait(); err != nil {
		return err
	}

	// root dir is handled by WriteWorkspaceRoot
	if relpath != "" {
		subdirRecord, serr := qwr.WriteDirectory(base, relpath, curDirRecords, ds)
		if serr != nil {
			return serr
		}
		curDirRecords = []*quantumfs.DirectoryRecord{subdirRecord}
	}

	//fmt.Printf("return Base: %s RelPath:%s Records: %v\n", base, relpath, curDirRecords)
	for _, dR := range curDirRecords {
		dirRecordChan <- dR
	}
	return nil
}

func upload(ds quantumfs.DataStore, wsdb quantumfs.WorkspaceDB,
	ws string, base string, relpath string) error {

	var topDirRecords []*quantumfs.DirectoryRecord

	if relpath == "" {
		enableExclChecks = true
	}
	fmt.Printf("Handling %s\n", filepath.Join(base, relpath))

	// setup the top level wait-group and channel to
	// receive DirectoryRecord information
	topWg, topCtx := errgroup.WithContext(context.Background())
	topDirRecChan := make(chan *quantumfs.DirectoryRecord)

	// start the top-level goroutine
	topWg.Go(func() error {
		return handleDir(topCtx, topDirRecChan, base, relpath, ds)
	})

	go func() {
		topWg.Wait()
		close(topDirRecChan)
	}()

	// collect DirectoryRecords over the channel
	for dirRecord := range topDirRecChan {
		topDirRecords = append(topDirRecords, dirRecord)
	}

	// dirRecord channel is now closed, check if the WaitGroup
	// exited prematurely due to errors
	if err := topWg.Wait(); err != nil {
		return err
	}

	fmt.Println("Completed handling dirs")
	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(base, topDirRecords, ds)
	if wsrErr != nil {
		return wsrErr
	}

	return qwr.CreateWorkspace(wsdb, ws, wsrKey)
}
