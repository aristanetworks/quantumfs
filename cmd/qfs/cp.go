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
	srcPath  string
	dstPath  string
	fileinfo os.FileInfo
}

// Use InsertInode to copy a directory tree from one workspace to another, possibly
// at different paths.
func cp() {
	if flag.NArg() != 3 {
		fmt.Println("Too few arguments to cp")
		os.Exit(exitBadArgs)
	}

	srcRoot := flag.Arg(1)
	dstRoot := flag.Arg(2)

	toProcess := make(chan copyItem, 10000)
	wg := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go insertPaths(toProcess, &wg)
	}

	err := filepath.Walk(srcRoot, func(srcPath string, fileinfo os.FileInfo,
		inErr error) error {

		if inErr != nil {
			panic(fmt.Sprintf("Error walking source %s: %v\n", srcPath,
				inErr))
		}

		dstPath := strings.Replace(srcPath, srcRoot, dstRoot, 1)

		toProcess <- copyItem{
			srcPath:  srcPath,
			dstPath:  dstPath,
			fileinfo: fileinfo,
		}

		return nil
	})

	wg.Wait()

	if err != nil {
		panic(fmt.Sprintf("Error walking %v\n", err))
	}
}

func insertPaths(paths chan copyItem, wg *sync.WaitGroup) {
	api, err := quantumfs.NewApi()
	if err != nil {
		panic(fmt.Sprintf("Unable to initialize API: %v\n", err))
	}

	for {
		job, stillWorking := <-paths
		if !stillWorking {
			wg.Done()
			return
		}

		src := job.srcPath
		dst := job.dstPath

		stat := job.fileinfo.Sys().(syscall.Stat_t)

		mode := uint(stat.Mode)

		if utils.BitFlagsSet(mode, syscall.S_IFDIR) {
			// Create directories
			err = os.Mkdir(dst, os.FileMode(mode)&os.ModePerm)

			// The directory may have been created by a child in a
			// concurrent goroutine.
			if err != nil && !os.IsExist(err) {
				panic(fmt.Sprintf("Mkdir error on %s: %v\n", dst,
					err))
			}
			err = os.Chmod(dst, os.FileMode(mode)&os.ModePerm)
			if err != nil {
				panic(fmt.Sprintf("Chmod error on %s: %v\n", dst,
					err))
			}
			err = os.Chown(dst, int(stat.Uid), int(stat.Gid))
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

		err = api.InsertInode(dst, string(key), stat.Mode, stat.Uid, stat.Gid)
		if err != nil {
			panic(fmt.Sprintf("InsertInode error on %s: %v\n", dst,
				err))
		}
	}
}
