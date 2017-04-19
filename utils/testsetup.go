// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "fmt"
import "io/ioutil"
import "os"
import "strings"

var rootDir string

func init() {
	// We must use a ramfs or else we get IO lag spikes of > 1 second
	rootDir = "/dev/shm/" + os.Getenv("ROOTDIRNAME")
	if !strings.Contains(rootDir, "-RootContainer-") {
		panic(fmt.Sprintf("Environmental variable ROOTDIRNAME is wrong: %s",
			rootDir))
	}

	err := os.MkdirAll(rootDir, 0777)
	if err != nil {
		panic(fmt.Sprintf("Unable to create temporary directories in"+
			" /dev/shm: %s", err.Error()))
	}
}

// Leave a record of the temporary directory's name
func SetupTestspace(itr int, testName string) (string, error) {
	var err error = nil
	var testRunDir string = ""

	for i := 0; i < itr; i++ {
		testRunDir, err = ioutil.TempDir(rootDir, testName)

		if err != nil {
			continue
		}
		if err = os.Chmod(testRunDir, 0777); err != nil {
			continue
		}
		return testRunDir, nil
	}

	return testRunDir, err
}
