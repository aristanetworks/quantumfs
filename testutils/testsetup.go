// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package testutils

import "fmt"
import "io/ioutil"
import "os"
import "strings"

import "github.com/aristanetworks/quantumfs/utils"

var rootDir string

func init() {
	// We must use a ramfs or else we get IO lag spikes of > 1 second
	rootDir = "/dev/shm/" + os.Getenv("ROOTDIRNAME")
	if !strings.Contains(rootDir, "-RootContainer-") {
		panic(fmt.Sprintf("Environmental variable ROOTDIRNAME is wrong: %s",
			rootDir))
	}

	err := utils.MkdirAll(rootDir, 0777)
	if err != nil {
		panic(fmt.Sprintf("Unable to create temporary directories in"+
			" /dev/shm: %s", err.Error()))
	}
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
		goto end
	}

	panic(fmt.Sprintf("Unable to create test directory: %v", err))
end:
	return testRunDir
}
