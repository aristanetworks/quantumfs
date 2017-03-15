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
	[]*qwr.HardLinkInfo, error) {

	var curDirRecords []*quantumfs.DirectoryRecord
	var curHlinks []*qwr.HardLinkInfo

	curDirPath := filepath.Join(base, dir)

	dirEnts, err := ioutil.ReadDir(curDirPath)
	if err != nil {
		return nil, nil, fmt.Errorf("Reading %s failed %s\n",
			curDirPath, err)
	}

	// recurse over dirs, ignore files
	for _, dirEnt := range dirEnts {
		if !dirEnt.IsDir() {
			continue
		}

		childRecords, childHlinks, derr := handleDir(curDirPath, dirEnt.Name(),
			ds)
		if derr != nil {
			return nil, nil, derr
		}
		fmt.Printf("Base: %s Dir:%s Records: %v\n", curDirPath, dirEnt.Name(), childRecords)
		curDirRecords = append(curDirRecords, childRecords...)
		curHlinks = append(curHlinks, childHlinks...)
	}

	// handle all files within cur dir
	for _, dirEnt := range dirEnts {
		if dirEnt.IsDir() {
			continue
		}

		record, hlink, ferr := qwr.WriteFile(ds, dirEnt,
			filepath.Join(curDirPath, dirEnt.Name()))
		if ferr != nil {
			return nil, nil, ferr
		}

		fmt.Printf("File:%s Records: %v\n", dirEnt.Name(), record)
		curDirRecords = append(curDirRecords, record)
		curHlinks = append(curHlinks, hlink)
	}

	// root dir is handled by WriteWorkspaceRoot
	if dir != "/" {
		subdirRecord, serr := qwr.WriteDirectory(base, dir, curDirRecords, ds)
		if serr != nil {
			return nil, nil, serr
		}
		curDirRecords = []*quantumfs.DirectoryRecord{subdirRecord}
	}

	fmt.Printf("return Base: %s Dir:%s Records: %v\n", base, dir, curDirRecords)
	return curDirRecords, curHlinks, nil
}

func upload(ds quantumfs.DataStore, wsdb quantumfs.WorkspaceDB,
	base string, ws string, dirs []string) error {

	var topDirRecords []*quantumfs.DirectoryRecord
	var topHlinks []*qwr.HardLinkInfo

	for _, dir := range dirs {
		fmt.Printf("Handling %s\n", filepath.Join(base, dir))
		dirRecords, hlinks, err := handleDir(base, dir, ds)
		if err != nil {
			return err
		}
		topDirRecords = append(topDirRecords, dirRecords...)
		topHlinks = append(topHlinks, hlinks...)
	}

	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(base, topDirRecords, topHlinks, ds)
	if wsrErr != nil {
		return wsrErr
	}

	return qwr.CreateWorkspace(wsdb, ws, wsrKey)
}
