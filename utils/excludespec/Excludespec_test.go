// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package excludespec

import "testing"

// Terminology:
//
// hierarchy = List of paths that should be setup prior to the test.
//	    The directory must be suffixed by "/"
// content = Content of exclude file
// expected = List of paths from hierarchy that are expected to be included,
//          along with the count of sub-paths

var syntaxTestHierarchy = []string{
	".file1",
	"dir1/",
	"dir2/.file2",
	"dir3/.file3a",
	"dir3/file3b",
}

func testSyntaxNoErr(test *testHelper, content string) {
	err := loadSpecTest(test.TempDir, syntaxTestHierarchy,
		content)
	test.AssertNoErr(err)
}

func testSyntaxErr(test *testHelper, content string) {
	err := loadSpecTest(test.TempDir, syntaxTestHierarchy,
		content)
	test.Assert(err != nil, "Expected err but got nil")
}

// Syntax tests.

func TestSyntax_Basic(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "dir1"
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_Comments(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
# some comment
dir2
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_EmptyLine(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1


dir2
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_IncludePathWithSlashSuffix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
+dir1/
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_MultipleWordsInOneLine(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "dir1 dir2"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestSyntax_ExcludePathNotExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "fileNotExist"
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_IncludePathNotExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
+dir99
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_Advanced(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
# some comment
dir1

+dir1/
.file1
dir3
+dir3
+dir3/.file3a
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_AbsolutePath(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "/dir1"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestSyntax_PathWithSuffix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "dir1/"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestSyntax_DotDotPrefix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "..dir1"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestSyntax_DoubleExclude(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
dir1
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestSyntax_DoubleInclude(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
+dir1
+dir1
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

// Exclude file processing tests.

// Tests where some or all content is excluded.
func TestEmptySpec(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11/",
		}
		content := ""
		expected := pathInfo{
			"/":             1,
			"dir1":          1,
			"dir1/subdir11": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeAll(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/subdir111a/",
			"dir1/subdir11a/file111b",
			"dir1/file11b",
			"file",
		}
		content := `
file
dir1
`
		expected := pathInfo{
			"/": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeSome(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/.file111a",
			"dir1/subdir11a/.file111b",
			"dir1/subdir11b",
			"file1",
			"file2",
		}
		content := `
file1
dir1/subdir11a/.file111a
`
		expected := pathInfo{
			"/":                        2,
			"file2":                    0,
			"dir1":                     2,
			"dir1/subdir11a":           1,
			"dir1/subdir11b":           0,
			"dir1/subdir11a/.file111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeAllIncludeSome(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/content111a",
			"dir1/subdir11a/content111b",
			"file1",
			"file2",
		}
		content := `
file1
file2
dir1
+dir1
+dir1/subdir11a
+dir1/subdir11a/content111b
+file1
`
		expected := pathInfo{
			"/":                          2,
			"file1":                      0,
			"dir1":                       1,
			"dir1/subdir11a":             1,
			"dir1/subdir11a/content111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeAllIncludeAll(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/content111a",
			"dir1/subdir11a/content111b",
		}
		content := `
dir1/subdir11a
+dir1/subdir11a/
`
		expected := pathInfo{
			"/":                          1,
			"dir1":                       1,
			"dir1/subdir11a":             2,
			"dir1/subdir11a/content111a": 0,
			"dir1/subdir11a/content111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeSomeIncludeAll(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/.file111a",
			"dir1/subdir11a/subdir111b/file1111c",
			"file1",
			"file2",
		}
		content := `
dir1/subdir11a/.file111a
+dir1/subdir11a/subdir111b
`
		expected := pathInfo{
			"/":                                   3,
			"file1":                               0,
			"file2":                               0,
			"dir1":                                1,
			"dir1/subdir11a":                      1,
			"dir1/subdir11a/subdir111b":           1,
			"dir1/subdir11a/subdir111b/file1111c": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeSomeIncludeSome(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/content111a",
			"dir1/subdir11a/content111b",
			"file1",
			"file2",
		}
		content := `
file1
file2
dir1/subdir11a
+dir1/subdir11a
+dir1/subdir11a/content111b
+file1
`
		expected := pathInfo{
			"/":                          2,
			"file1":                      0,
			"dir1":                       1,
			"dir1/subdir11a":             1,
			"dir1/subdir11a/content111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// any missing paths should be ignored and not cause any bad sideeffects
func TestExcludeSomeIncludeSomeMissingPaths(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/content111a",
			"dir1/subdir11a/content111b",
			"file1",
			"file2",
		}
		content := `
file1
file2
# missing exclude path
file3
dir1/subdir11a
+dir1/subdir11a
# missing include path
+dir99
+dir1/subdir11a/content111b
+file1
`
		expected := pathInfo{
			"/":                          2,
			"file1":                      0,
			"dir1":                       1,
			"dir1/subdir11a":             1,
			"dir1/subdir11a/content111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestIncludeAllIncludeSome(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `

dir1
+dir1/subdir11/
+dir1/subdir11/file
`
		expected := pathInfo{
			"/":                  1,
			"dir1":               1,
			"dir1/subdir11":      1,
			"dir1/subdir11/file": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeAllExcludeSome(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `

dir1/subdir11
dir1/file
`
		expected := pathInfo{
			"/":    1,
			"dir1": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// Tests where the content being excluded or included overlaps with
// previous directive.
func TestExcludeDouble(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `
dir1/subdir11
dir1/subdir11
`
		expected := pathInfo{
			"/":         1,
			"dir1":      1,
			"dir1/file": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestIncludeDouble(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `

dir1
+dir1/subdir11
+dir1/subdir11
`
		expected := pathInfo{
			"/":             1,
			"dir1":          1,
			"dir1/subdir11": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeImplicitlyExcluded(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `

dir1
dir1/subdir11
`
		expected := pathInfo{
			"/": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestIncludeImplicitlyIncluded(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file",
			"dir1/subdir11/file",
		}
		content := `
+dir1/subdir11
`
		expected := pathInfo{
			"/":                  1,
			"dir1":               2,
			"dir1/file":          0,
			"dir1/subdir11":      1,
			"dir1/subdir11/file": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// Tests where same prefix exists for directory or file names.
func TestExcludeNameMatch(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file11a",
			"dir1/file11b",
			"dir1b/file11a",
			"dir1bc/file11a",
			"file1",
			"file11",
		}
		content := `
dir1
dir1b
file1
`
		expected := pathInfo{
			"/":              2,
			"file11":         0,
			"dir1bc":         1,
			"dir1bc/file11a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// Miscellaneous tests.
func TestIncludeNameMatch(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/file11a",
			"dir1/file11b",
			"dir1b/file11a",
			"dir1bc/file11a",
			"file1",
			"file11",
		}
		content := `
dir1
dir1b
dir1bc
file1
file11
+dir1bc/file11a
+file1
`
		expected := pathInfo{
			"/":              2,
			"file1":          0,
			"dir1bc":         1,
			"dir1bc/file11a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestBasicSpec(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"dir1/subdir1/",
			"dir2/subdir2/",
		}

		content := `
# exclude dir1 and all its content
dir1
`
		expected := pathInfo{
			"/":            1,
			"dir2":         1,
			"dir2/subdir2": 0,
		}

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExcludeEmptyDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir1/",
		}
		content := "dir1/subdir1"
		expected := pathInfo{
			"/":    1,
			"dir1": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestIncludeEmptyDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/",
		}
		content := `
dir1/subdir11a
+dir1/subdir11a
`
		expected := pathInfo{
			"/":              1,
			"dir1":           1,
			"dir1/subdir11a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestAutoIncludeAncestors(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"dir1/dir11/",
		}
		content := `
dir1
+dir1/dir11
`
		expected := pathInfo{
			"/":          1,
			"dir1":       1,
			"dir1/dir11": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// TestIncludeNotExcludedParent includes a parent directory which is not
// excluded by a preceeding exclude directive unlike
// TestIncludeImplicitlyIncluded which includes a path that was never
// excluded.
func TestIncludeNotExcludedParent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"dir1/dir11a/file111a",
		}
		content := `
dir1/dir11a/file111a
+dir1/dir11a/file111a
`
		expected := pathInfo{
			"/":                    1,
			"dir1":                 1,
			"dir1/dir11a":          1,
			"dir1/dir11a/file111a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// Advanced scenario based tests.
func TestScenario1Spec(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/.file1",
			"dir1/subdir11a/.file2",
			"dir1/subdir11a/file3",
			"dir2/subdir22a/file1",
			"dir2/subdir22b/file1",
			"dir2/subdir22c/file1",
			"dir3/subdir33c/file1",
			"file1",
			"file2",
		}

		content := `
# missing exclude path
dir98

# exclude everything under dir1/subdir11a
# except dir1/subdir11a/.file1 and dir1/subdir11a/.file2
dir1/subdir11a
+dir1/subdir11a/.file1
+dir1/subdir11a/.file2

# exclude everything under dir2 but keep the directory
# names (no content) dir2/subdir22a and dir2/subdir22c
dir2
+dir2/subdir22a
+dir2/subdir22b

# exclude file1
file1

#missing include path
+dir99
`
		expected := pathInfo{
			"/": 4,

			"dir1":                  1,
			"dir1/subdir11a":        2,
			"dir1/subdir11a/.file1": 0,
			"dir1/subdir11a/.file2": 0,

			"dir2":           2,
			"dir2/subdir22a": 0,
			"dir2/subdir22b": 0,

			"dir3":                 1,
			"dir3/subdir33c":       1,
			"dir3/subdir33c/file1": 0,

			"file2": 0,
		}

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

// Path globbing tests.
func TestGlobbingSpec(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/blah1/dir2/file1",
			"dir1/blah2/dir2/file1",
			"dir1/blah3/dir2/file1",
			"dir1/blah4/dir2/file1",
			"dir1/blah1/dir2/file2",
			"dir2/blah2/dir2/file1",
			"dir1/blah3/dir3/file1",
		}

		content := `
# exclude everything under dir1 apart from explicit includes
dir1
dir2
+dir1/*/dir2/file1
`
		expected := pathInfo{
			"/": 1,

			"dir1":                  4,
			"dir1/blah1":            1,
			"dir1/blah1/dir2":       1,
			"dir1/blah1/dir2/file1": 0,

			"dir1/blah2":            1,
			"dir1/blah2/dir2":       1,
			"dir1/blah2/dir2/file1": 0,

			"dir1/blah3":            1,
			"dir1/blah3/dir2":       1,
			"dir1/blah3/dir2/file1": 0,

			"dir1/blah4":            1,
			"dir1/blah4/dir2":       1,
			"dir1/blah4/dir2/file1": 0,

		}

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}
