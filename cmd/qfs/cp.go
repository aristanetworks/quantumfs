// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type copyItem struct {
	srcPath            string
	dstPath            string
	dstRootPrefix      string
	dstWorkspacePrefix string
	fileinfo           os.FileInfo
}

// Use InsertInode to copy a directory tree from one workspace to another, possibly
// at different paths.
func cp() {
	if flag.NArg() != 3 {
		fmt.Println("Too few arguments to cp")
		os.Exit(exitBadArgs)
	}

	srcRoot, err := filepath.Abs(flag.Arg(1))
	if err != nil {
		panic(fmt.Sprintf("Error making src absolute: %s/%v\n", flag.Arg(1),
			err))
	}
	dstRoot, err := filepath.Abs(flag.Arg(2))
	if err != nil {
		panic(fmt.Sprintf("Error making dst absolute: %s/%v\n", flag.Arg(2),
			err))
	}

	// Find the workspace name of the destination
	dstParts := strings.Split(dstRoot, "/")

	qfsRoot := ""
	i := 0
	for ; i < len(dstParts); i++ {
		qfsRoot += "/" + dstParts[i]
		var stat syscall.Stat_t
		err := syscall.Stat(qfsRoot+"/"+quantumfs.ApiPath, &stat)
		if err != nil {
			continue // no "api" file
		}

		if stat.Ino == quantumfs.InodeIdApi {
			break
		}
	}

	dstWorkspacePrefix := strings.Join(dstParts[i+1:], "/")

	toProcess := make(chan copyItem, 10000)
	wg := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go insertPaths(toProcess, &wg)
	}

	err = filepath.Walk(srcRoot, func(srcPath string, fileinfo os.FileInfo,
		inErr error) error {

		if inErr != nil {
			panic(fmt.Sprintf("Error walking source %s: %v\n", srcPath,
				inErr))
		}

		dstPath := strings.TrimPrefix(srcPath, srcRoot)

		toProcess <- copyItem{
			srcPath:            srcPath,
			dstPath:            dstPath,
			dstRootPrefix:      dstRoot,
			dstWorkspacePrefix: dstWorkspacePrefix,
			fileinfo:           fileinfo,
		}

		return nil
	})

	close(toProcess)

	wg.Wait()

	if err != nil {
		panic(fmt.Sprintf("Error walking %v\n", err))
	}
}

func insertPaths(jobs chan copyItem, wg *sync.WaitGroup) {
	api, err := quantumfs.NewApi()
	if err != nil {
		panic(fmt.Sprintf("Unable to initialize API: %v\n", err))
	}

	for job := range jobs {
		src := job.srcPath
		dst := job.dstPath
		rootPrefix := job.dstRootPrefix
		workspacePrefix := job.dstWorkspacePrefix

		stat := job.fileinfo.Sys().(*syscall.Stat_t)

		mode := uint(stat.Mode)

		// We may run before our parent directory has been created, create it
		// anyways and the permissions will be fixed up when that directory
		// is eventually processed.
		err = utils.MkdirAll(filepath.Dir(rootPrefix+dst), 0777)
		if err != nil && !os.IsExist(err) {
			// This may not end up being fatal
			fmt.Printf("MkdirAll error making parents for %s: %v\n",
				dst, err)
		}

		if utils.BitFlagsSet(mode, syscall.S_IFDIR) {
			// Create directories
			err = os.Mkdir(rootPrefix+dst, os.FileMode(mode)&os.ModePerm)

			// The directory may have been created by a child in a
			// concurrent goroutine.
			if err != nil && !os.IsExist(err) {
				panic(fmt.Sprintf("Mkdir error on %s: %v\n", dst,
					err))
			}
			err = os.Chmod(rootPrefix+dst, os.FileMode(mode)&os.ModePerm)
			if err != nil {
				panic(fmt.Sprintf("Chmod error on %s: %v\n", dst,
					err))
			}
			err = os.Chown(rootPrefix+dst, int(stat.Uid), int(stat.Gid))
			if err != nil {
				panic(fmt.Sprintf("Chown error on %s: %v\n", dst,
					err))
			}

			continue
		}

		// InsertInode all other inode types
		_, err, key := utils.LGetXattr(src, quantumfs.XAttrTypeKey,
			quantumfs.ExtendedKeyLength)
		if err != nil {
			panic(fmt.Sprintf("LGetXattr error on %s: %v\n", src, err))
		}

		err = api.InsertInode(workspacePrefix+dst, string(key), stat.Mode,
			stat.Uid, stat.Gid)
		if err != nil {
			panic(fmt.Sprintf("InsertInode error on %s: %v\n", dst,
				err))
		}
	}
	wg.Done()
}
