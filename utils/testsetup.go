// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "fmt"
import "io/ioutil"
import "os"

var rootDir string

func init() {
	// We must use a ramfs or else we get IO lag spikes of > 1 second
	rootDir = "/dev/shm/" + os.Getenv("ROOTDIRNAME")

	err := os.MkdirAll(rootDir, 0777)
	if err != nil {
		panic(fmt.Sprintf("Unable to create temporary directories in"+
			" /dev/shm: %v", err))
	}
}

// Leave a record of the temperary directory's name
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
