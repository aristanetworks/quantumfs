// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package testutils

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/aristanetworks/quantumfs/utils"
)

var rootDir string

func init() {
	// We must use a ramfs or else we get IO lag spikes of > 1 second
	rootDir = "/dev/shm/" + os.Getenv("ROOTDIRNAME")
	if !strings.Contains(rootDir, "-RootContainer-") {
		rootDir = "/dev/shm/" + os.Getenv("USER") + "-RootContainer-" +
			strconv.Itoa(os.Getppid())
	}

	if err := utils.MkdirAll(rootDir, 0777); err != nil {
		panic(fmt.Sprintf("Unable to create temporary directories in"+
			" /dev/shm: %s", err.Error()))
	}

	// The newly created directory's perm is (mode & ~umask & 0777)
	// Make sure it is 0777 no matter what umask is
	if err := os.Chmod(rootDir, 0777); err != nil {
		panic(fmt.Sprintf("Unable to chmod rootDir "+
			" %s: %s", rootDir, err.Error()))
	}

	// Enable utils.DeferableRwMutex recursive grab checking
	utils.CheckForRecursiveRLock = true
}

// Leave a record of the temporary directory's name
func SetupTestspace(testName string) string {
	var err error = nil
	var testRunDir string = ""

	for i := 0; i < 10; i++ {
		testRunDir, err = ioutil.TempDir(rootDir, testName)

		if err != nil {
			continue
		}
		if err = os.Chmod(testRunDir, 0777); err != nil {
			continue
		}
		return testRunDir
	}

	panic(fmt.Sprintf("Unable to create test directory: %v", err))
}
