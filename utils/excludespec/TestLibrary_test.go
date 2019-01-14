// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

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

	"github.com/aristanetworks/quantumfs/testutils"
)

type testHelper struct {
	testutils.TestHelper
}

// exclude spec test
type esTest func(*testHelper)

func runTest(t *testing.T, test esTest) {
	t.Parallel()
	// call-stack until test should be
	// 1 <testname>
	// 0 runTestCommon
	testName := testutils.TestName(1)
	th := &testHelper{
		TestHelper: testutils.NewTestHelper(testName,
			testutils.TestRunDir, t),
	}
	defer th.EndTest()

	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

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
		// test cases mark directory with trailing slash
		// to differentiate files and directory paths
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
	// walker walks the hierarchy anchored at base.
	// It checks if a path is excluded as per the exclude file
	// referred by filename.
	// All non-excluded (ie included) paths are tracked for
	// comparison with the expected paths.
	// For included directories, the RecordCount API is used
	// to know the number of directory records that should be
	// present for the included directory. This record count is also
	// checked against the expected path information.
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		var testDirRelPath string
		testDirRelPath, err = filepath.Rel(base, path)
		if err != nil {
			return err
		}

		// paths in the base directory are treated as slash by
		// exclude spec processing
		if testDirRelPath == "." {
			testDirRelPath = "/"
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

const excludeFileName = "testExcludeFileName"

func loadSpecTest(testdir string, hierarchy []string,
	content string) error {

	filename := filepath.Join(testdir, excludeFileName)
	datadir := filepath.Join(testdir, "data")

	err := createHierarchy(datadir, hierarchy)
	if err != nil {
		return fmt.Errorf("IO error during test data creation: %s",
			err)
	}
	err = writeExcludeSpec(filename, content)
	if err != nil {
		return fmt.Errorf("IO error during exclude file write: %s",
			err)
	}
	_, err = LoadExcludeInfo(datadir, filename)
	return err
}

func runSpecTest(testdir string, hierarchy []string,
	content string, expected pathInfo) error {

	filename := filepath.Join(testdir, "testExcludeFileName")
	datadir := filepath.Join(testdir, "data")

	err := createHierarchy(datadir, hierarchy)
	if err != nil {
		return fmt.Errorf("IO error during test data creation: %s",
			err)
	}
	err = writeExcludeSpec(filename, content)
	if err != nil {
		return fmt.Errorf("IO error during exclude file write: %s",
			err)
	}
	err = testSpec(datadir, filename, expected)
	if err != nil {
		return fmt.Errorf("Failed: %s\n", err)
	}
	return nil
}
