// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/testutils"
)

func runBench(t *testing.B, test func(*testHelper)) {
	t.StopTimer()

	testName := testutils.TestName(1) + strconv.Itoa(time.Now().Nanosecond())
	th := &testHelper{
		TestHelper: TestHelper{
			TestHelper: testutils.NewTestHelper(testName,
				TestRunDir, nil),
		},
	}

	th.CreateTestDirs()

	config := th.defaultConfig()
	defer th.EndTest()

	startChan := make(chan struct{}, 0)
	th.startQuantumFs(config, startChan, false)
	<-startChan

	test(th)
}

func parallelWorkerTest(t *testing.B, th *testHelper,
	createFn func(string) []string) {

	workspace := th.NewWorkspace()
	relPath := th.RelPath(workspace)

	parallelThreads := 40
	numTasks := 100

	files := createFn(workspace)

	// Branch our base workspace
	workspaces := make([]string, numTasks, numTasks)
	api := th.getApi()
	for i := 0; i < numTasks; i++ {
		wsrPath := "test/test/workspace" + strconv.Itoa(i)
		api.Branch(relPath, wsrPath)
		api.EnableRootWrite(wsrPath)
		workspaces[i] = th.AbsPath(wsrPath)
	}

	tasks := make(chan string, numTasks)
	var wg sync.WaitGroup
	work := func() {
		for wsr := range tasks {
			for _, file := range files {
				// Read the file
				_, err := ioutil.ReadFile(wsr + "/" + file)
				if err != nil {
					panic("Failed to read file")
				}
			}
		}
		wg.Done()
	}

	// Now we setup workers to act on our N files
	for i := 0; i < parallelThreads; i++ {
		wg.Add(1)

		go work()
	}

	t.ResetTimer()
	t.StartTimer()

	for _, wsr := range workspaces {
		tasks <- wsr
	}
	close(tasks)

	wg.Wait()
	t.StopTimer()
}

func BenchmarkShallowTree(t *testing.B) {
	runBench(t, func(th *testHelper) {
		parallelWorkerTest(t, th, func(workspace string) []string {
			fileData := string(GenData(150))

			// N is the number of files we have to instantiate - so
			// that's how many we need to create in our base workspace
			files := make([]string, t.N, t.N)
			for i := 0; i < t.N; i++ {
				filename := "file" + strconv.Itoa(i)
				files[i] = filename
				testutils.PrintToFile(workspace+"/"+filename,
					fileData)
			}

			return files
		})
	})
}

func makeDirsInDir(numDirs int, nodesPerDir int, wsr string) []string {
	if numDirs <= 1 {
		// Base case, just use the wsr directory
		os.MkdirAll(wsr+"/baseDir", 0777)
		return []string{"baseDir"}
	}

	numParentDirs := int(math.Ceil(float64(numDirs) / float64(nodesPerDir)))
	directories := makeDirsInDir(numParentDirs, nodesPerDir, wsr)

	curDir := 0
	curDirCount := 0
	dirs := make([]string, numDirs, numDirs)
	for i := 0; i < numDirs; i++ {
		dirpath := directories[curDir] + "/" + strconv.Itoa(i)
		os.MkdirAll(wsr+"/"+dirpath, 0777)
		dirs[i] = dirpath

		curDirCount++
		if curDirCount >= nodesPerDir {
			curDir++
			curDirCount = 0
		}
	}

	return dirs
}

func makeFilesInDir(numFiles int, nodesPerDir int, wsr string) []string {
	fileData := string(GenData(150))

	//Spread the files across the directories we have
	numDirectories := int(math.Ceil(float64(numFiles) / float64(nodesPerDir)))
	directories := makeDirsInDir(numDirectories, nodesPerDir, wsr)

	curDir := 0
	curDirCount := 0
	files := make([]string, numFiles, numFiles)
	for i := 0; i < numFiles; i++ {
		filepath := directories[curDir] + "/" + strconv.Itoa(i)
		testutils.PrintToFile(wsr+"/"+filepath, fileData)
		files[i] = filepath

		curDirCount++
		if curDirCount >= nodesPerDir {
			curDir++
			curDirCount = 0
		}
	}

	return files
}

func BenchmarkBalancedTree(t *testing.B) {
	runBench(t, func(th *testHelper) {
		parallelWorkerTest(t, th, func(workspace string) []string {
			// Sqrt twice should give us about three levels
			// in the workspace tree
			nodesPerDir := int(math.Sqrt(math.Sqrt(float64(t.N))))
			if nodesPerDir <= 1 {
				// There must always be allowed at least two nodes
				// per dir
				nodesPerDir = 2
			}

			return makeFilesInDir(t.N, nodesPerDir, workspace)
		})
	})
}

func BenchmarkTallTree(t *testing.B) {
	runBench(t, func(th *testHelper) {
		parallelWorkerTest(t, th, func(workspace string) []string {

			// Make the tallest tree possible - one file per dir
			return makeFilesInDir(t.N, 2, workspace)
		})
	})
}
