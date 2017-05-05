// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package excludespec

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// prints the actual and expected maps side by side so they
// are easy to visually compare
func dumpMaps(actual pathInfo, expected pathInfo) string {
	var dump bytes.Buffer

	// extract keys so they can be sorted
	epaths := make([]string, 0)
	apaths := make([]string, 0)
	for path := range expected {
		epaths = append(epaths, path)
	}
	sort.Strings(epaths)
	for path := range actual {
		apaths = append(apaths, path)
	}
	sort.Strings(apaths)
	paths := epaths
	if len(apaths) > len(epaths) {
		paths = apaths
	}
	fmt.Fprintf(&dump, "%30s %30s\n",
		"Expected path:count",
		"Actual path:count")
	for idx := range paths {
		var apath, epath, acount, ecount string
		if idx >= len(apaths) {
			apath = "-"
			acount = "-"
		} else {
			apath = apaths[idx]
			acount = strconv.Itoa(actual[apath])
		}
		if idx >= len(epaths) {
			epath = "-"
			ecount = "-"
		} else {
			epath = epaths[idx]
			ecount = strconv.Itoa(expected[epath])
		}

		fmt.Fprintf(&dump, "%30s:%s %30s:%s\n",
			epath, ecount,
			apath, acount)
	}
	return dump.String()
}

func createHierarchy(base string, paths []string) error {
	var err error
	for _, path := range paths {
		err = os.MkdirAll(filepath.Join(base, filepath.Dir(path)),
			os.ModePerm)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, "/") {
			err = os.MkdirAll(filepath.Join(base, path),
				os.ModePerm)
			if err != nil {
				return err
			}
			continue
		}
		err = ioutil.WriteFile(filepath.Join(base, path), nil, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeExcludeSpec(path string, content string) error {
	return ioutil.WriteFile(path, []byte(content), os.ModePerm)
}

// Count of sub-paths per path
// Files have the count as zero while directories have the count equal to the
// number of files and subdirectories under that directory
type pathInfo map[string]int

func testSpec(base string, filename string, expected pathInfo) error {

	actual := make(pathInfo)

	exInfo, err := LoadExcludeInfo(base, filename)
	if err != nil {
		return err
	}
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		var testDirRelPath string
		testDirRelPath, err = filepath.Rel(base, path)
		if err != nil {
			return err
		}

		// in expected test outputs we don't track base dir itself
		if testDirRelPath == "." {
			return nil
		}

		if exInfo.PathExcluded(testDirRelPath) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		actual[testDirRelPath] = 0
		if info.IsDir() {
			ents, err := ioutil.ReadDir(path)
			if err != nil {
				return err
			}
			actual[testDirRelPath] = exInfo.RecordCount(testDirRelPath,
				len(ents))
		}
		return nil
	}

	err = filepath.Walk(base, walker)
	if err != nil {
		return fmt.Errorf("IO error during test: %v", err)
	}

	if !reflect.DeepEqual(actual, expected) {
		details := fmt.Sprintf("%s\n%s\n",
			dumpMaps(actual, expected),
			exInfo)
		return fmt.Errorf("actual != expected. Details: \n%s\n",
			details)
	}
	return nil
}

type testCleaner struct {
	testdir string
	t       *testing.T
}

func (c *testCleaner) cleanup() {
	err := os.RemoveAll(c.testdir)
	if err != nil {
		c.t.Fatalf("Test %q cleanup failed: %v",
			c.t.Name(), err)
	}
}

func setup(t *testing.T) *testCleaner {
	dir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Test %q setup failed: %v",
			t.Name(), err)
	}
	return &testCleaner{testdir: dir}
}

const excludeFileName = "testExcludeFileName"

func loadSpecTest(t *testing.T, testdir string, hierarchy []string,
	content string) error {

	filename := filepath.Join(testdir, excludeFileName)
	datadir := filepath.Join(testdir, "data")

	err := createHierarchy(datadir, hierarchy)
	if err != nil {
		t.Fatalf("IO error during test data creation: %s",
			err)
	}
	err = writeExcludeSpec(filename, content)
	if err != nil {
		t.Fatalf("IO error during exclude file write: %s",
			err)
	}
	_, err = LoadExcludeInfo(datadir, filename)
	return err
}

func runSpecTest(t *testing.T, testdir string, hierarchy []string,
	content string, expected pathInfo) {

	filename := filepath.Join(testdir, "testExcludeFileName")
	datadir := filepath.Join(testdir, "data")

	err := createHierarchy(datadir, hierarchy)
	if err != nil {
		t.Fatalf("IO error during test data creation: %s",
			err)
	}
	err = writeExcludeSpec(filename, content)
	if err != nil {
		t.Fatalf("IO error during exclude file write: %s",
			err)
	}
	err = testSpec(datadir, filename, expected)
	if err != nil {
		t.Fatalf("Failed: %s\n", err)
	}
}

type loadSpecTestCase struct {
	descr   string // test description
	content string // content of exclude file
	err     bool   // true if error expected else false
}

func loadSpecTestRunner(testcases []loadSpecTestCase, hierarchy []string,
	topTest *testing.T) {

	for _, test := range testcases {
		topTest.Run(test.descr, func(t *testing.T) {
			// test.descr isn't suited as testdir name
			// so we use topTest's name
			tc := setup(topTest)
			defer tc.cleanup()
			err := loadSpecTest(t, tc.testdir, hierarchy,
				test.content)
			if !test.err && err != nil {
				fmt.Println("Unexpected error: ", err)
				t.Fatal()
			}
			if test.err && err == nil {
				fmt.Println("Expected error but got nil")
				t.Fatal()
			}
		})
	}
}
