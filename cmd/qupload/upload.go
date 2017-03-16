// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "fmt"
import "io/ioutil"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

// by default exclusion list is not checked
var enableExclChecks = false

func handleDirContents(base string, relpath string,
	ds quantumfs.DataStore) ([]*quantumfs.DirectoryRecord, error) {

	var curDirRecords []*quantumfs.DirectoryRecord

	curDirPath := filepath.Join(base, relpath)
	dirEnts, err := ioutil.ReadDir(curDirPath)
	if err != nil {
		return nil, fmt.Errorf("Reading %s failed %s\n",
			curDirPath, err)
	}

	// recurse over dirs, ignore files
	for _, dirEnt := range dirEnts {
		if !dirEnt.IsDir() {
			continue
		}
		if enableExclChecks && utils.IsPathExcluded(
			filepath.Join(relpath, dirEnt.Name())) {
			continue
		}
		childRecords, derr := handleDir(curDirPath, dirEnt.Name(),
			ds)
		if derr != nil {
			return nil, derr
		}
		//fmt.Printf("Base: %s Dir:%s Records: %v\n", curDirPath, dirEnt.Name(), childRecords)
		curDirRecords = append(curDirRecords, childRecords...)
	}

	// handle all files within cur dir
	for _, dirEnt := range dirEnts {
		if dirEnt.IsDir() {
			continue
		}

		if enableExclChecks && utils.IsPathExcluded(
			filepath.Join(relpath, dirEnt.Name())) {
			continue
		}
		record, ferr := qwr.WriteFile(ds, dirEnt,
			filepath.Join(curDirPath, dirEnt.Name()))
		if ferr != nil {
			return nil, ferr
		}

		//fmt.Printf("File:%s Records: %v\n", dirEnt.Name(), record)
		curDirRecords = append(curDirRecords, record)
	}

	return curDirRecords, nil
}

func handleDir(base string, relpath string,
	ds quantumfs.DataStore) ([]*quantumfs.DirectoryRecord,
	error) {

	var curDirRecords []*quantumfs.DirectoryRecord

	// this relpath is included, check if it's contents
	// are excluded
	if !enableExclChecks || !utils.IsPathExcluded(relpath+"/") {
		records, err := handleDirContents(base, relpath, ds)
		if err != nil {
			return nil, err
		}
		curDirRecords = append(curDirRecords, records...)
	}

	// root dir is handled by WriteWorkspaceRoot
	if relpath != "" {
		subdirRecord, serr := qwr.WriteDirectory(base, relpath, curDirRecords, ds)
		if serr != nil {
			return nil, serr
		}
		curDirRecords = []*quantumfs.DirectoryRecord{subdirRecord}
	}

	//fmt.Printf("return Base: %s RelPath:%s Records: %v\n", base, relpath, curDirRecords)
	return curDirRecords, nil
}

func upload(ds quantumfs.DataStore, wsdb quantumfs.WorkspaceDB,
	ws string, base string, relpath string) error {

	var topDirRecords []*quantumfs.DirectoryRecord

	if relpath == "" {
		enableExclChecks = true
	}
	fmt.Printf("Handling %s\n", filepath.Join(base, relpath))
	dirRecords, err := handleDir(base, relpath, ds)
	if err != nil {
		return err
	}
	topDirRecords = append(topDirRecords, dirRecords...)
	fmt.Println("Completed handling dirs")
	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(base, topDirRecords, ds)
	if wsrErr != nil {
		return wsrErr
	}

	return qwr.CreateWorkspace(wsdb, ws, wsrKey)
}
