// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

/*
Package excludespec provides APIs for loading a specification file that
describes which files or directories are excluded and/or included.
*/
package excludespec

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const excludeTopDir = "/"

const HelpText = `
Exclude spec file should be formatted as follows

 - One path per line.
 - Comments and empty lines are allowed. A comment is any line that starts with "#".
 - Path may or may not exist. Path must be relative to the base directory.
 - Shell globbing syntax is allowed.
 - Absolute paths are not allowed.
 - Exclude path must not be "/" suffixed.
 - Path to be included is prefixed with "+".
 - The order of exclude and include paths in the file does not matter.
 - Use ordering that makes the file more readable.
 - All excludes are processed first and then includes are processed.

A path is considered to be included if it meets following criteria:
 - if the path is not covered by any exclude directives (implict inclusion)
 - if the path has been explicitly included

Consider a base directory called "/rootdir". Some examples of exclude files
are listed below. All paths used in the examples are under the base
directory "rootdir". In other words, the absolute path for dir1 is
"/rootdir/dir1".

 Example-1:

 # exclude dir1 and all its contents
 dir1
 # include only dir1 (not its contents)
 +dir1
 # include dir1/subdir1 (not its contents)
 +dir1/subdir1
 # include dir1/subdir1/file1
 +dir1/subdir1/file1
 # include dir1/subdir4 (not its contents)
 +dir1/subdir4
 # include contents of dir1/subdir3/subsubdir1
 +dir1/subdir3/subsubdir3/

 Example-2:

 # exclude dir2/subdir2
 # implicitly includes dir2 and all other content in dir2
 dir2/subdir2
 # include selective content dir2/subdir2/file2
 # inside excluded content (dir2/subdir2)
 +dir2/subdir2/file2

 Example-3:

 # exclude dir2
 dir2
 # include specific file from all subdirs of dir2
 +dir2/*/file
`

var empty struct{}

type ExcludeInfo struct {
	// temporary maps to collect exclude and include paths
	excludes map[string]struct{}
	// true if only the dir should be included
	// false if the dir and all its content should be included
	includes map[string]bool

	// Sorting words ensures that a directory is listed
	// before all its sub-directories. Checking if ancestor
	// is included works reliably when parent path is listed before
	// any of the child paths.
	sortedExcWords []string
	sortedIncWords []string

	excludeRE *regexp.Regexp // exclude path regexp
	includeRE *regexp.Regexp // include path regexp

	// desired count of records for a path
	dirRecordCounts map[string]int
}

/*
Overview of scheme:
- Parse the exclude spec file to collect words into exclude and include maps.
- Words are exclude and include directives.
- Build exclude and include regexp based on words.
- Build record count for paths in exclude spec.
- PathExcluded uses exclude and include regexps.
- RecordCount uses the record count info.
*/

func LoadExcludeInfo(base string, filename string) (*ExcludeInfo, error) {

	var exInfo ExcludeInfo

	exInfo.dirRecordCounts = make(map[string]int)
	exInfo.excludes = make(map[string]struct{})
	exInfo.includes = make(map[string]bool)
	exInfo.sortedExcWords = make([]string, 0)
	exInfo.sortedIncWords = make([]string, 0)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	lineno := 0
	for s.Scan() {
		line := s.Text()
		line = strings.TrimSpace(line)
		lineno++
		// ignore comments and empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		includePath := isIncludePath(line)

		// Paths are obtained when line in exclude file passes
		// parsing (syntax checks) and glob expansion.
		paths, err := parseExcludeLine(base, line)
		if err == errIgnorePath {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, err)
		}
		for _, path := range paths {
			addWord(&exInfo, path, includePath)
		}
	}

	if err = s.Err(); err != nil {
		return nil, err
	}

	err = processWords(&exInfo, base)
	if err != nil {
		return nil, fmt.Errorf("processing words failed: %v",
			err)
	}

	return &exInfo, nil
}

func isIncludePath(word string) bool {
	return strings.HasPrefix(word, "+")
}

var errIgnorePath = fmt.Errorf("ignore this path")

func parseExcludeLine(base string, line string) ([]string, error) {
	parts := strings.Split(line, " ")
	hasTrailingSlash := strings.HasSuffix(line, "/")
	// rules
	switch {
	case len(parts) > 1:
		return nil, fmt.Errorf("whitespace in a line")
	// the slash and dot-dot are mainly to make sure that all paths
	// are relative and under the base directory
	case strings.HasPrefix(line, "/"):
		return nil, fmt.Errorf("path has / prefix")
	case strings.HasPrefix(line, ".."):
		return nil, fmt.Errorf("path has .. prefix")
	case !isIncludePath(line) && hasTrailingSlash:
		// Exclude directive implies that both directory and its
		// contents are excluded so there is no need for using
		// slash suffix. Since include directives have specific
		// behavior of slash suffix to avoid confusion, slash suffixes
		// for exclude directives are prohibited.
		return nil, fmt.Errorf("exclude path has / suffix")
	}

	// Perform glob expansion, ignoring the path on any error
	// if path is not ignored then computation of recordcount
	// gets impacted. In other words, only valid paths are setup
	// in the regexp checker
	files, err := filepath.Glob(
		filepath.Join(base, strings.TrimPrefix(parts[0], "+")))
	if err != nil {
		return nil, errIgnorePath
	}
	// Convert paths to relative after globbing, add trailing slash if needed
	for i, file := range files {
		files[i], err = filepath.Rel(base, file)
		if err != nil {
			return nil, fmt.Errorf("Cannot convert path to relative")
		}
		if hasTrailingSlash {
			files[i] += "/"
		}
	}
	return files, nil
}

// Keep track of words by adding them to include and exclude maps.
// Separating addition and processing of words, simplifies maintaining
// regex. Otherwise, things like duplicate words need to be handled
// in a special way.
func addWord(exInfo *ExcludeInfo, word string, includePath bool) {
	if !includePath {
		if _, exists := exInfo.excludes[word]; !exists {
			exInfo.excludes[word] = empty
			exInfo.sortedExcWords = append(exInfo.sortedExcWords,
				word)
			sort.Strings(exInfo.sortedExcWords)
		}
		return
	}
	dirOnly := true
	if strings.HasSuffix(word, "/") {
		dirOnly = false
	}
	word = strings.TrimSuffix(word, "/")
	if _, exists := exInfo.includes[word]; !exists {
		curParent := word
		for {
			parent := getParent(curParent)
			if parent == "/" {
				break
			}
			if _, ok := exInfo.includes[parent]; !ok {
				exInfo.includes[parent] = true
				exInfo.sortedIncWords =
					append(exInfo.sortedIncWords, parent)
			}
			curParent = parent
		}
		exInfo.includes[word] = dirOnly
		exInfo.sortedIncWords = append(exInfo.sortedIncWords,
			word)
		sort.Strings(exInfo.sortedIncWords)
	}
	return
}

// Returns true if new content will be included by "word".
// In other words, when the content referred by "word" is excluded
// and not included.
func includingNewContent(exInfo *ExcludeInfo, word string, dirOnly bool) bool {
	excluded := false
	included := false

	if exInfo.excludeRE != nil {
		excluded = exInfo.excludeRE.MatchString(word)
	}

	if !excluded && !dirOnly {
		path := word + "/"
		for _, exWord := range exInfo.sortedExcWords {
			if strings.HasPrefix(exWord, path) {
				excluded = true
				break
			}
		}
	}

	if excluded && exInfo.includeRE != nil {
		included = exInfo.includeRE.MatchString(word)
	}

	return excluded && !included
}

// Explicitly excluded path must not get included.
// Thus following is invalid:
//
// d1/d2/d3
// +d1/
//
func explicitExcludeTurnsInclude(exInfo *ExcludeInfo, path string,
	onlyDir bool) error {

	words := make([]string, 0)
	words = append(words, "^"+regexp.QuoteMeta(path)+"$")
	if !onlyDir {
		words = append(words, "^"+regexp.QuoteMeta(path)+"/")
	}
	re, err := compileRE(words)
	if err != nil {
		return err
	}

	for _, exWord := range exInfo.sortedExcWords {
		if exWord != path && re.MatchString(exWord) {
			return fmt.Errorf("explicit exclude %q now included",
				exWord)
		}
	}
	return nil
}

// Setup regex and record counts.
func processWords(exInfo *ExcludeInfo, base string) error {
	var err error
	reWords := make([]string, 0)

	for _, word := range exInfo.sortedExcWords {
		// skip paths that are already excluded
		if exInfo.PathExcluded(word) {
			continue
		}
		// Avoid dir10 regex to match dir103
		// by adding ^dir10$ and ^dir10/
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"$")
		// Adding trailing slash even for file excludes
		// is ok since file excludes will match it.
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"/")
		excludeSetRecordCount(exInfo, base, word)
		exInfo.excludeRE, err = compileRE(reWords)
		if err != nil {
			return err
		}
	}

	// Include paths which refer to a file or directory-without content
	// will have a dollar suffix in regex.
	// Include paths which refer to directory and its nested content will
	// have both dollar suffixed and slash suffixed entries in regex.
	reWords = make([]string, 0)
	for _, word := range exInfo.sortedIncWords {
		onlyDir := exInfo.includes[word]

		err = explicitExcludeTurnsInclude(exInfo, word, onlyDir)
		if err != nil {
			return err
		}
		// If no new content is being included then don't process
		// the include directive. This keeps the later coder simple.
		if !includingNewContent(exInfo, word, onlyDir) {
			continue
		}
		// A path can be included only if all its ancestors
		// are included.
		parent := getParent(word)
		if parent != excludeTopDir {
			// root is never excluded
			if exInfo.PathExcluded(parent) {
				// As ancestors are auto-included by
				// include directives, this condition is a bug
				panic(fmt.Sprintf("Ancestor(s) of %q not "+
					"included. Details: %s", word, exInfo))
			}
		}
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"$")
		if !onlyDir {
			reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"/")
		}
		// alter RE as we build regex so that PathExcluded check
		// can be used while building RE
		exInfo.includeRE, err = compileRE(reWords)
		if err != nil {
			return err
		}
		// Only parent's record count is affected.
		// Ancestor record count is not affected.
		// It's guaranteed that parent exists in dirRecordCounts
		// since all explicitly included paths and their
		// ancestors are present.
		includeSetRecordCount(exInfo, base, word)
	}
	return nil
}

func compileRE(s []string) (*regexp.Regexp, error) {
	var re *regexp.Regexp
	var err error
	// empty s creates a nil Regexp
	if len(s) != 0 {
		res := strings.Join(s, "|")
		re, err = regexp.Compile(res)
		if err != nil {
			return nil, err
		}
	}
	return re, nil
}

func initRecordCount(exInfo *ExcludeInfo, dir string, path string) error {
	dirEnts, dirErr := ioutil.ReadDir(filepath.Join(dir, path))
	if dirErr != nil {
		return fmt.Errorf("ReadDir failed for %s error: %v",
			dir, dirErr)
	}
	exInfo.dirRecordCounts[path] = len(dirEnts)
	return nil
}

func excludeSetRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	// since path is excluded, its record count must be zero
	exInfo.dirRecordCounts[path] = 0
	// An exclude path only says that the last path-component
	// is excluded and hence only its parent's record count is
	// affected, grand-parent's record count is unaffected.
	parent := getParent(path)
	// setup parent record count for the first time
	_, exist := exInfo.dirRecordCounts[parent]
	if !exist {
		err := initRecordCount(exInfo, base, parent)
		if err != nil {
			return err
		}
	}
	exInfo.dirRecordCounts[parent]--
	return nil
}

func getParent(path string) string {
	parent := filepath.Dir(path)
	if parent == "." {
		parent = excludeTopDir
	}
	return parent
}

func includeSetRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	onlyDir, _ := exInfo.includes[path]
	exInfo.dirRecordCounts[path] = 0
	if !onlyDir {
		// If directory with its sub-dirs is being included
		// then its record count needs to be set up.
		err := initRecordCount(exInfo, base, path)
		if err != nil {
			return err
		}
	}
	// Only parent's record count is affected.
	// Ancestor record count is not affected.
	// It's guaranteed that parent exists in dirRecordCounts
	// since all explicitly included paths and their
	// ancestors are present
	parent := getParent(path)
	exInfo.dirRecordCounts[parent]++
	return nil
}

// PathExcluded returns true if path is excluded as per exclude
// file spec.
func (e *ExcludeInfo) PathExcluded(path string) bool {
	// A path is excluded if theres a match in excludeRE
	// and no match in includeRE.
	// If a path doesn't match excludeRE then no need to check
	// includeRE.

	// by default everything is included
	excl := false
	if e.excludeRE != nil {
		excl = e.excludeRE.MatchString(path)
	}
	if excl && e.includeRE != nil {
		incl := e.includeRE.MatchString(path)
		excl = excl && !incl
	}
	return excl
}

// RecordCount returns the number of records nested under a path
// after applying the exclude spec.
func (e *ExcludeInfo) RecordCount(path string, recs int) int {
	exrecs, exist := e.dirRecordCounts[path]
	if !exist {
		return recs
	}
	return exrecs
}

// String returns the string representation of the exclude information.
// It is useful for debugging.
func (exInfo *ExcludeInfo) String() string {
	var dump bytes.Buffer

	fmt.Fprintf(&dump, "Dump of exclude information\n")
	fmt.Fprintf(&dump, "Include map: %v\n", exInfo.includes)
	fmt.Fprintf(&dump, "Exclude map: %v\n", exInfo.excludes)
	fmt.Fprintf(&dump, "Record counts: %v\n", exInfo.dirRecordCounts)
	if exInfo.excludeRE != nil {
		fmt.Fprintf(&dump, "Exclude Regex: %s\n", exInfo.excludeRE.String())
	} else {
		fmt.Fprintf(&dump, "Exclude Regex is nil\n")
	}
	if exInfo.includeRE != nil {
		fmt.Fprintf(&dump, "Include Regex: %s\n", exInfo.includeRE.String())
	} else {
		fmt.Fprintf(&dump, "Include Regex is nil\n")
	}
	return dump.String()
}
