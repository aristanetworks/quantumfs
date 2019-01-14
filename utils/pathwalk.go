// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

import (
	"os"
	"path/filepath"
)

// Like filepath.Walk(), but don't traverse into the directory at all if it is
// skipped. Does not walk in lexical order.
func Pathwalk(root string, walkFn filepath.WalkFunc) error {
	fileInfo, err := os.Lstat(root)

	err = walkFn(root, fileInfo, err)
	if err == filepath.SkipDir {
		return nil
	} else if err != nil {
		return err
	} else if !fileInfo.IsDir() {
		return nil
	}

	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()

	children, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, name := range children {
		err = Pathwalk(root+"/"+name, walkFn)
		if err != nil {
			return err
		}
	}

	return nil
}
