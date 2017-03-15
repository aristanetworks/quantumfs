// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "fmt"
import "io/ioutil"
import "path/filepath"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"

func handleDir(base string, dir string,
	ds quantumfs.DataStore) ([]*quantumfs.DirectoryRecord,
	error) {

	var curDirRecords []*quantumfs.DirectoryRecord

	curDirPath := filepath.Join(base, dir)
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
		childRecords, derr := handleDir(curDirPath, dirEnt.Name(),
			ds)
		if derr != nil {
			return nil, derr
		}
		fmt.Printf("Base: %s Dir:%s Records: %v\n", curDirPath, dirEnt.Name(), childRecords)
		curDirRecords = append(curDirRecords, childRecords...)
	}

	// handle all files within cur dir
	for _, dirEnt := range dirEnts {
		if dirEnt.IsDir() {
			continue
		}
		record, ferr := qwr.WriteFile(ds, dirEnt,
			filepath.Join(curDirPath, dirEnt.Name()))
		if ferr != nil {
			return nil, ferr
		}

		fmt.Printf("File:%s Records: %v\n", dirEnt.Name(), record)
		curDirRecords = append(curDirRecords, record)
	}

	// root dir is handled by WriteWorkspaceRoot
	if dir != "/" {
		subdirRecord, serr := qwr.WriteDirectory(base, dir, curDirRecords, ds)
		if serr != nil {
			return nil, serr
		}
		curDirRecords = []*quantumfs.DirectoryRecord{subdirRecord}
	}

	fmt.Printf("return Base: %s Dir:%s Records: %v\n", base, dir, curDirRecords)
	return curDirRecords, nil
}

func upload(ds quantumfs.DataStore, wsdb quantumfs.WorkspaceDB,
	base string, ws string, dirs []string) error {

	var topDirRecords []*quantumfs.DirectoryRecord

	for _, dir := range dirs {
		fmt.Printf("Handling %s\n", filepath.Join(base, dir))
		dirRecords, err := handleDir(base, dir, ds)
		if err != nil {
			return err
		}
		topDirRecords = append(topDirRecords, dirRecords...)
	}
	fmt.Println("Completed handling dirs")
	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(base, topDirRecords, ds)
	if wsrErr != nil {
		return wsrErr
	}

	return qwr.CreateWorkspace(wsdb, ws, wsrKey)
}
