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

	dirEnts, err := ioutil.ReadDir(dir)
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
		curDirRecords = append(curDirRecords, childRecords...)
		curHlinks = append(curHlinks, childHlinks...)
	}

	// handle all files within cur dir
	for _, dirEnt := range dirEnts {
		if dirEnt.IsDir() {
			continue
		}

		record, hlink, ferr := qwr.WriteFile(ds,
			filepath.Join(curDirPath, dirEnt.Name()))
		if ferr != nil {
			return nil, nil, ferr
		}

		curDirRecords = append(curDirRecords, record)
		curDirHlinks = append(curDirHlinks, hlink)
	}

	// root dir is handled by WriteWorkspaceRoot
	if dir != "/" {
		subdirRecord, serr := WriteDirectory(base, dir, curDirRecords, ds)
		if serr != nil {
			return nil, nil, serr
		}
		curDirRecords = []*quantumfs.DirectoryRecord{subdirRecord}
	}

	return curDirRecords, curHlinks, nil
}

func upload(ds quantumfs.DataStore, wsdb quantumfs.WorkspaceDB,
	base string, ws string, dirs []string) error {

	var topDirRecords []*quantumfs.DirectoryRecord
	var topHlinks []*qwr.HardLinkInfo

	for _, dir := range dirs {
		dirRecords, hlinks, err := handleDir(base, dir, ds)
		if err != nil {
			return err
		}
		topDirRecords = append(topDirRecords, dirRecords...)
		topHlinks = append(topHlinks, hlinks...)
	}

	wsrKey, wsrErr := qwr.WriteWorkspaceRoot(base, topDirRecords, hlinks, ds)
	if wsrErr != nil {
		return wsrErr
	}

	return wsdb.CreateWorkspace(wsdb, ws, wsrKey)
}
