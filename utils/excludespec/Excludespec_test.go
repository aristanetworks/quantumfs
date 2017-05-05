// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package excludespec

import "testing"

func TestExcludeSyntax(t *testing.T) {
	hierarchy := []string{
		".file1",
		"dir1",
		"dir2/.file2",
		"dir3/.file3a",
		"dir3/file3b",
	}

	testContent := []loadSpecTestCase{
		// no error cases
		{"basic syntax", "dir1", false},
		{"comments", "dir1\n# some comment\ndir2", false},
		{"emptyline", "dir1\n\n\n\ndir2", false},
		{"include path has / suffix", "dir1\n+dir2/", false},
		{"advanced synctax",
			"# some comment\n" +
				"dir1\n+dir2/\n.file1\n+dir3\n+dir3/.file3a",
			false},

		// error cases
		{"whitespace in line", "dir1 dir2", true},
		{"path has /", "/dir1", true},
		{"path has ..", "dir1\n..dir2", true},
		{"exclude path has / suffix", "dir1/\n+dir2/", true},
		{"invalid path", "dirabc\n+efg", true},
	}

	loadSpecTestRunner(testContent, hierarchy, t)
}

func TestExcludeRules(t *testing.T) {
	hierarchy := []string{
		"dir1/.file1",
	}

	testContent := []loadSpecTestCase{
		// no error cases
		{"dir included before file include",
			"dir1\n+dir1\n+dir1/.file1", false},

		// error case
		{"dir not included before file include",
			"dir1\n+dir1/.file1", true},
	}

	loadSpecTestRunner(testContent, hierarchy, t)
}

/*
hierarchy = List of paths that should be setup prior to the test.
	    The directory must be suffixed by "/"
content = Content of exclude file
expected = List of paths from hierarchy that are expected to be included,
           along with the count of sub-paths for a directory
*/

// TestBasicExclude tests the basic exclude file
func TestBasicExclude(t *testing.T) {
	tc := setup(t)
	defer tc.cleanup()

	hierarchy := []string{
		"dir1/subdir1/",
		"dir2/subdir2/",
	}

	content := `
# exclude all dir1 and all its content
dir1
`
	expected := pathInfo{
		"dir2":         1,
		"dir2/subdir2": 0,
	}

	runSpecTest(t, tc.testdir, hierarchy, content, expected)
}

// TestBasicExclude tests the advanced exclude file
func TestAdvancedExclude(t *testing.T) {
	tc := setup(t)
	defer tc.cleanup()

	hierarchy := []string{
		// subdirs

		"dir1/subdir11a/",
		"dir2/subdir22a/subdir222a/",
		"dir2/subdir22a/file222b",
		"dir3/subdir33a/.file333a",
		"dir3/subdir33a/.file333b",
		"dir4/subdir44a/",
		"dir5/subdir55a/content555a",
		"dir5/subdir55a/content555b",
		"dir6/subdir66a/content666a",
		"dir6/subdir66a/content666b",

		// dirs
		"dir7/",
		"dir8/content88a",
		"dir8/content88b",
		"dir9/.file99a",
		"dir9/Xfile99b",
		"dir10/",
		"dir11/content1111a",
		"dir11/content1111b",
		"dir12/content1212a",
		"dir12/content1212b",
	}

	content := `
# tests for subdirs
#exclude empty subdir
dir1/subdir11a

#exclude non-empty subdir, all content
dir2/subdir22a

#exclude non-empty subdir, selective content
dir3/subdir33a/.file333a

#include an excluded empty subdir
dir4/subdir44a
+dir4
+dir4/subdir44a

#include an excluded subdir, with all contents
dir5/subdir55a
+dir5
+dir5/subdir55a/

#include an excluded subdir, with selective content
dir6/subdir66a
+dir6
+dir6/subdir66a
+dir6/subdir66a/content666b

#exclude empty dir
dir7

#exclude non-empty dir, all content
dir8

#exclude non-empty dir, selective content
dir9/.file99a

#include an excluded empty dir
dir10
+dir10

#include an excluded dir, with all contents
dir11
+dir11/

#include an excluded dir, with selective content
dir12
+dir12
+dir12/content1212b


`
	expected := pathInfo{
		"dir1":                     0,
		"dir2":                     0,
		"dir3":                     1,
		"dir3/subdir33a":           1,
		"dir3/subdir33a/.file333b": 0,
		"dir4":                       1,
		"dir4/subdir44a":             0,
		"dir5":                       1,
		"dir5/subdir55a":             2,
		"dir5/subdir55a/content555a": 0,
		"dir5/subdir55a/content555b": 0,
		"dir6":                       1,
		"dir6/subdir66a":             1,
		"dir6/subdir66a/content666b": 0,

		"dir9":               1,
		"dir9/Xfile99b":      0,
		"dir10":              0,
		"dir11":              2,
		"dir11/content1111a": 0,
		"dir11/content1111b": 0,
		"dir12":              1,
		"dir12/content1212b": 0,
	}

	runSpecTest(t, tc.testdir, hierarchy, content, expected)
}
