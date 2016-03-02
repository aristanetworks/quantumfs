// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "fmt"
import "os"
import "strings"
import "syscall"

// This file contains all the functions which are used by qfs (and other
// applications) to perform special quantumfs operations. Primarily this is done by
// marhalling the arguments and passing them to quantumfsd for processing, then
// interpretting the results.

func NewApi() *Api {
	api := Api{}

	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	directories := strings.Split(cwd, "/")
	path := ""

	for {
		path = strings.Join(directories, "/") + "/" + ApiPath
		stat, err := os.Lstat(path)
		if err != nil {
			if len(directories) == 1 {
				// We didn't find anything and hit the root, give up
				panic("Couldn't find api file")
			}
			directories = directories[:len(directories)-1]
			continue
		}
		if !stat.IsDir() && stat.Size() == 0 {
			stat_t := stat.Sys().(*syscall.Stat_t)
			if stat_t.Ino == InodeIdApi {
				// No real filesystem is likely to give out inode 2
				// for a random file but quantumfs reserves that
				// inode for all the api files.
				break
			}
		}
		continue
	}

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	api.fd = fd
	if err != nil {
		panic(err)
	}

	return &api
}

type Api struct {
	fd *os.File
}

// branch the src workspace into a new workspace called dst.
func (api *Api) Branch(src string, dst string) error {
	if slashes := strings.Count(src, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", src)
	}

	if slashes := strings.Count(dst, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", dst)
	}

	return nil
}
