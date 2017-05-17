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

// --- exclude file syntax tests ---
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

func TestExcludeSyntax_BasicSyntax(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "dir1"
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_Comments(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1
# some comment
dir2
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_EmptyLine(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir1


dir2
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_IncludePathWithSlashSuffix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
dir2
+dir2/
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_AdvancedSyntax(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := `
# some comment
dir1

.file1
dir3
+dir3/
`
		testSyntaxNoErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_AbsolutePath(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "/dir1"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_ExcludePathWithSuffix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "dir1/"
		testSyntaxErr(test, excludeFileContent)
	})
}

func TestExcludeSyntax_DotDotPrefix(t *testing.T) {
	runTest(t, func(test *testHelper) {
		excludeFileContent := "..dir1"
		testSyntaxErr(test, excludeFileContent)
	})
}

// --- exclude file processing tests ---

// TestBasicExclude tests the basic exclude file
func TestBasicExclude(t *testing.T) {
	runTest(t, func(test *testHelper) {
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

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_EmptySubdir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir1/subdir11a/",
		}
		content := "dir1/subdir11a"
		expected := pathInfo{
			"dir1": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_NonEmptySubdirAllContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir2/subdir22a/subdir222a/",
			"dir2/subdir22a/file222b",
		}
		content := "dir2/subdir22a"
		expected := pathInfo{
			"dir2": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_NonEmptySubdirSelectiveContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir3/subdir33a/.file333a",
			"dir3/subdir33a/.file333b",
		}
		content := "dir3/subdir33a/.file333a"
		expected := pathInfo{
			"dir3":                     1,
			"dir3/subdir33a":           1,
			"dir3/subdir33a/.file333b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedSubdirEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir4/subdir44a/",
		}
		content := `
dir4/subdir44a
+dir4/subdir44a
`
		expected := pathInfo{
			"dir4":           1,
			"dir4/subdir44a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedSubdirAllContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir5/subdir55a/content555a",
			"dir5/subdir55a/content555b",
		}
		content := `
dir5/subdir55a
+dir5/subdir55a/
`
		expected := pathInfo{
			"dir5":                       1,
			"dir5/subdir55a":             2,
			"dir5/subdir55a/content555a": 0,
			"dir5/subdir55a/content555b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedSubdirSelectiveContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir6/subdir66a/content666a",
			"dir6/subdir66a/content666b",
		}
		content := `
dir6/subdir66a
+dir6/subdir66a
+dir6/subdir66a/content666b
`
		expected := pathInfo{
			"dir6":                       1,
			"dir6/subdir66a":             1,
			"dir6/subdir66a/content666b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_EmptyDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir7/",
		}
		content := "dir7"
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_NonEmptyDirAllContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir8/content88a",
			"dir8/content88b",
		}
		content := "dir8"
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_NonEmptyDirSelectiveContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir9/.file99a",
			"dir9/Xfile99b",
		}
		content := "dir9/.file99a"
		expected := pathInfo{
			"dir9":          1,
			"dir9/Xfile99b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedEmptyDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir10/",
		}
		content := `
dir10
+dir10
`
		expected := pathInfo{
			"dir10": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedDirAllContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir11/content1111a",
			"dir11/content1111b",
		}
		content := `
dir11
+dir11/
`
		expected := pathInfo{
			"dir11":              2,
			"dir11/content1111a": 0,
			"dir11/content1111b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_IncludeExcludedDirSelectiveContent(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir12/content1212a",
			"dir12/content1212b",
		}
		content := `

dir12
+dir12
+dir12/content1212b
`
		expected := pathInfo{
			"dir12":              1,
			"dir12/content1212b": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_DirnameOverlap(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir13/file1313a",
			"dir13/file1313b",
			"dir13b/file1313a",
			"dir13bc/file1313a",
		}
		content := `

dir13
dir13bc
`
		expected := pathInfo{
			"dir13b":           1,
			"dir13b/file1313a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_FilenameOverlap(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir14/file",
			"dir14/fileABC",
			"dir14b/file",
			"dir14bc/file",
		}
		content := `

dir14/file
dir14b/file
+dir14b/file
`
		expected := pathInfo{
			"dir14":         1,
			"dir14/fileABC": 0,
			"dir14b":        1,
			"dir14b/file":   0,
			"dir14bc":       1,
			"dir14bc/file":  0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_EmptySpec(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir15/subdir1515/file151515a",
		}
		content := ""
		expected := pathInfo{
			"dir15":                        1,
			"dir15/subdir1515":             1,
			"dir15/subdir1515/file151515a": 0,
		}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}

func TestExclude_ParentNotIncluded(t *testing.T) {
	runTest(t, func(test *testHelper) {

		hierarchy := []string{
			"dir15/subdir1515/file151515a",
		}
		content := `
dir15
+dir15/subdir1515
`
		expected := pathInfo{}
		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.Assert(err != nil, "include of subdir passed even when parent "+
			"not included")
	})

}

// TestBasicExclude tests the advanced exclude file
func TestAdvancedExclude(t *testing.T) {
	runTest(t, func(test *testHelper) {

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
			"dir13/file1313a",
			"dir13/file1313b",
			"dir13b/file1313a",
			"dir13bc/file1313a",
			"dir14/file",
			"dir14/fileABC",
			"dir14b/file",
			"dir14bc/file",
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
+dir4/subdir44a

#include an excluded subdir, with all contents
dir5/subdir55a
+dir5/subdir55a/

#include an excluded subdir, with selective content
dir6/subdir66a
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

# dirname overlap
dir13
dir13bc

# filename overlap
dir14/file
dir14b/file
+dir14b/file
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
			"dir13b":             1,
			"dir13b/file1313a":   0,
			"dir14":              1,
			"dir14/fileABC":      0,
			"dir14b":             1,
			"dir14b/file":        0,
			"dir14bc":            1,
			"dir14bc/file":       0,
		}

		err := runSpecTest(test.TempDir, hierarchy, content, expected)
		test.AssertNoErr(err)
	})
}
