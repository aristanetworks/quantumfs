// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package utils package is for generic blobs of codes used in ether.
package utils

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

//Semaphore implements semaphore using chan
type Semaphore chan int

const semTimeout time.Duration = 60 * time.Second
const exitCCMarker int64 = -1

// P acquires a resources
func (s Semaphore) P() {

	timer := time.NewTimer(semTimeout)
	defer timer.Stop()

	select {
	case s <- 1:
	case <-timer.C:
		panic(fmt.Sprintf("Timeout in Semaphore.P() after %v of waiting", semTimeout))
	}
}

// V releases a resources
func (s Semaphore) V() {
	timer := time.NewTimer(semTimeout)
	defer timer.Stop()

	select {
	case <-s:
	case <-timer.C:
		panic(fmt.Sprintf("Timeout in Semaphore.V() after %v of waiting", semTimeout))
	}
}

// HumanizeBytes returns a string representing the size
// suffixed with human readable units like B=bytes, KB=kilobytes etc
func HumanizeBytes(size uint64) string {

	suffix := []string{"B", "KB", "MB", "GB"}

	f := float64(size)
	var i int
	for i = 0; f >= 1024 && i < len(suffix); i++ {
		f = f / 1024
	}

	if i == len(suffix) {
		i = i - 1
	}

	return fmt.Sprintf("%.1f %s", f, suffix[i])
}

// BytesXferredProgressBar is the information
type BytesXferredProgressBar struct {
	CC chan int64

	group sync.WaitGroup
}

// StartBytesXferredProgressBar starts a progress bar
// which listens for bytes transferred (int64) on channel CC.
// The progress is updated whenever an update is received on
// the channel or every delay duration
func StartBytesXferredProgressBar(totalSize uint64, show bool,
	delay time.Duration) *BytesXferredProgressBar {

	pbar := BytesXferredProgressBar{}
	pbar.CC = make(chan int64)

	var current uint64
	start := time.Now()
	timeout := time.Tick(delay)

	go func() {
		pbar.group.Add(1)
		defer pbar.group.Done()

		if show {
			fmt.Println()
			defer fmt.Println()
		}

		for {
			showProgress(totalSize, current, start, show)
			select {
			case written := <-pbar.CC:
				current += uint64(written)
				// 0 is the zero value for int64 and also a valid number of
				// bytes written by fileCopy (eg: source file has no content)
				// hence use an exit marker
				if written == exitCCMarker {
					return
				}
			case <-timeout:
			}
		}
	}()

	return &pbar
}

// StopBytesXferredProgressBar stops the progress bar and
// waits for the associated goroutines to exit
func StopBytesXferredProgressBar(pbar *BytesXferredProgressBar) {

	pbar.CC <- exitCCMarker
	pbar.group.Wait()
}

func showProgress(totalSize uint64, current uint64, start time.Time,
	progress bool) {

	if !progress {
		return
	}

	speed := "crazy fast"

	now := time.Now()
	if uint64(now.Sub(start).Seconds()) > 0 {
		speed = HumanizeBytes(current/uint64(now.Sub(start).Seconds())) + "/s"
	}
	// log.Printf is thread-safe but it doesn't handle \r
	fmt.Printf("\r[Started] %10s [Copied] %10s [Done] %3d%% [Speed] %10s [Duration] %10s",
		start.Format(time.Kitchen), HumanizeBytes(current),
		(current*100)/totalSize, speed, time.Since(start))
}

// CreateTestDirContents creates a temporary source directory hierarchy
// and populates it with files and directories. It returns
// testSrcDir, testDstDir, error. The caller is responsible
// to cleanup testSrcDir and testDstDir
func CreateTestDirContents() (string, string, error) {

	rand.Seed(time.Now().UTC().UnixNano())
	fileContent := []string{
		"Some dummy data 0xAB123CDE",
		"",
	}

	// make sure to have the parent dirs listed before listing file
	paths := []string{
		"testDir10/",
		"testDir20/testDir21/",
		"testDir20/testDir21/testFile22",
		"testDir30/",
		"testDir30/testFile31",
		"testDir30/testDir31/testDir32/",
	}

	testSrcDir, err := ioutil.TempDir("/tmp", "test-src-")
	if err != nil {
		return "", "", err
	}

	testDstDir, err := ioutil.TempDir("/tmp", "test-dst-")
	if err != nil {
		return "", "", err
	}

	// create the test directories and files under testSrcDir
	for _, p := range paths {
		if strings.HasSuffix(p, "/") {
			err = os.MkdirAll(filepath.Join(testSrcDir, p), os.ModePerm)
		} else {
			bytes := []byte(fileContent[rand.Intn(len(fileContent))])
			err = ioutil.WriteFile(filepath.Join(testSrcDir, p), bytes, os.ModePerm)
		}
		if err != nil {
			return "", "", err
		}
	}

	return testSrcDir, testDstDir, err
}

// GetFiles returns the list of files within a directory
func GetFiles(srcRoot string) ([]string, error) {

	var files []string

	err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

// GetSubDirs returns a list of directories within a directory
func GetSubDirs(srcRoot string) ([]string, error) {

	var dirs []string

	err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if path == srcRoot {
			return nil
		}

		if info.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})

	return dirs, err
}

// GetDirSize returns the size in bytes of a directory hierarchy
func GetDirSize(path string) (uint64, error) {

	var totalSize uint64

	findSizeFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		totalSize += uint64(info.Size())
		return nil
	}

	err := filepath.Walk(path, findSizeFunc)
	return totalSize, err
}
