// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package excludespec

import "testing"

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

// --- syntax tests ---
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

func TestSyntax_FileNotExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "fileNotExist"
		testSyntaxErr(test, excludeFileContent)
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
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestSyntax_DoubleInclude(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
+dir1
+dir1
`
		testSyntaxErr(test, excludeFileContent)
	})
}

// --- exclude file processing tests ---
//
//hierarchy = List of paths that should be setup prior to the test.
//	    The directory must be suffixed by "/"
//content = Content of exclude file
//expected = List of paths from hierarchy that are expected to be included,
//          along with the count of sub-paths for a directory

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

func TestExcludeSelective(t *testing.T) {
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

func TestIncludeAll(t *testing.T) {
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

func TestExcludeAllIncludeSelective(t *testing.T) {
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

func TestExcludeSelectiveIncludeSelective(t *testing.T) {
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

// --- exclude file processing negative tests ---
func TestOnlyIncludes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"errdir1/errdir1111/",
		}
		content := "+dir1"
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.Assert(err != nil, "re-included path did not fail")
	})
}

func TestExcludeAllIncludeAllReinclude(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"dir1/dir11/",
		}
		content := `
dir1
+dir1
+dir1/
`
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.Assert(err != nil, "re-included path did not fail")
	})
}

func TestIncludeAncestorNotIncluded(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hierarchy := []string{
			"dir1/dir11/",
		}
		content := `
dir1
+dir1/dir11
`
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.Assert(err != nil, "include succeeded withour ancestors "+
			"being included")
	})
}

func TestImplicitlyIncludedParent(t *testing.T) {
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

// --- advanced error free spec ---
func TestAdvanceSpec(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			// exclude empty dir
			"dir1/subdir11a/",

			// exclude selectively
			"dir2/subdir22a/.file222a",
			"dir2/subdir22a/.file222b",
			"dir2/subdir22b",
			"file1",
			"file2",

			// include empty dir
			"dir3/subdir33a/",

			// include all
			"dir4/subdir44a/content444a",
			"dir4/subdir44a/content444b",

			// include selectively
			"dir5/subdir55a/content555a",
			"dir5/subdir55a/content555b",

			// exclude all
			"dir6/subdir66a/file666a",
			"dir6/subdir66b/",

			// implicitly include parent
			"dir7/dir77a/file777a",
		}

		content := `
# exclude empty subdir
dir1/subdir11a

# exclude selectively
file1
dir2/subdir22a/.file222a

# include empty subdir
dir3/subdir33a
+dir3/subdir33a/

# include all content
dir4
+dir4/

# include selectively
dir5
+dir5
+dir5/subdir55a
+dir5/subdir55a/content555b

# exclude all
dir6

# implictly include parent
dir7/dir77a/file777a
+dir7/dir77a/file777a
`
		expected := pathInfo{
			"/": 7,

			"dir1": 0,

			"dir2":                     2,
			"dir2/subdir22a":           1,
			"dir2/subdir22b":           0,
			"dir2/subdir22a/.file222b": 0,
			"file2":                    0,

			"dir3":           1,
			"dir3/subdir33a": 0,

			"dir4":                       1,
			"dir4/subdir44a":             2,
			"dir4/subdir44a/content444a": 0,
			"dir4/subdir44a/content444b": 0,

			"dir5":                       1,
			"dir5/subdir55a":             1,
			"dir5/subdir55a/content555b": 0,

			"dir7":                 1,
			"dir7/dir77a":          1,
			"dir7/dir77a/file777a": 0,
		}

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}
