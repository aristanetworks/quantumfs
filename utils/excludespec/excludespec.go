// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

/*
Package excludespec provides APIs for loading a specification file that
describes which files or directories are excluded and/or included. The spec
file must follow rules outlined below:

One path per line.
Comments and empty lines are allowed. A comment is any line that starts with "#".
Order of the paths in the file is important.
Path must be under the base directory specified.
Absolute paths are not allowed.
Exclude paths must not be "/" suffixed.
Paths to be included are prefixed with "+".
The parent and grand-parents of paths to be included must be included already.

Example:

dir1
+dir1/subdir1
+dir1/subdir1/file1
+dir1/subdir3
+dir1/subdir4
+dir1/subdir3/subsubdir4/

In the above example, anything under directory "dir1" is excluded except
dir1/subdir1/file1, dir1/subdir4 and dir1/subdir3/subsubdir4 and its contents.
Note contents of dir1/subdir3 and dir1/subdir4 are not included.
*/
package excludespec

import "bufio"
import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "path/filepath"
import "regexp"
import "strings"

const excludeTopDir = "/"

var empty struct{}

type ExcludeInfo struct {
	// temporary maps to collect exclude and include paths
	excludes map[string]struct{}
	// true if only the dir should be included
	// false if the dir and all its content should be included
	includes map[string]bool

	excludeRE *regexp.Regexp // exclude path regexp
	includeRE *regexp.Regexp // include path regexp

	// desired count of records for a path
	dirRecordCounts map[string]int
}

func isIncludePath(word string) bool {
	return strings.HasPrefix(word, "+")
}

func parseExcludeLine(base string, line string) (string, error) {
	parts := strings.Split(line, " ")
	// rules
	switch {
	case len(parts) > 1:
		return "", fmt.Errorf("whitespace in a line")
	// the slash and dot-dot are mainly to make sure that all paths
	// are relative and under the base directory
	case strings.HasPrefix(line, "/"):
		return "", fmt.Errorf("path has / prefix")
	case strings.HasPrefix(line, ".."):
		return "", fmt.Errorf("path has .. prefix")
	case !isIncludePath(line) && strings.HasSuffix(line, "/"):
		return "", fmt.Errorf("exclude path has / suffix")
	}
	// check to ensure the path entry in exclude file is valid
	_, serr := os.Lstat(filepath.Join(base, strings.TrimPrefix(line, "+")))
	if serr != nil {
		return "", serr
	}
	return parts[0], nil
}

func checkExcludeRules(exInfo *ExcludeInfo, word string) error {
	// parent and all grand-parent paths should be included
	// prioer to including a path
	if isIncludePath(word) {
		word = strings.TrimSuffix(word, "/")
		for d := filepath.Dir(word); d != "."; d = filepath.Dir(d) {
			d = strings.TrimPrefix(d, "+")
			if _, ok := exInfo.includes[d]; !ok {
				return fmt.Errorf("parent path is not included")
			}
		}
	}
	return nil
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

// Keep track of words by adding them to include and exclude maps.
// This helps in doing the regex generation and record count setup
// after all words have been added.
func addWord(exInfo *ExcludeInfo, word string, includePath bool) {
	if !includePath {
		exInfo.excludes[word] = empty
		// if an exclude overrides includes
		// clean up includes that are redundant now
		for inc, _ := range exInfo.includes {
			if strings.Contains(inc, word) {
				delete(exInfo.includes, inc)
				// record counts aren't setup yet
				// so no need to clean them up
			}
		}
		return
	}
	if !strings.HasSuffix(word, "/") {
		exInfo.includes[word] = true
		return
	}

	// including a hierarchy implies that
	// the regex for this word must not include $
	// ie dir and all its content must be included
	exInfo.includes[strings.TrimSuffix(word, "/")] = false
}

func excludeSetRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	exInfo.dirRecordCounts[path] = 0
	// an exclude path only says that the last path-component
	// is excluded and hence only its parent's record count is
	// impacted, grand-parents remain unaffected by this path
	// update the parent's record count
	parent := filepath.Dir(path)
	if parent == "." {
		parent = excludeTopDir
	}
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

func includeSetRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	onlyDir, _ := exInfo.includes[path]
	if !onlyDir {
		// if directory with complete content is being included
		// then its record count shouldn't be manipulated
		err := initRecordCount(exInfo, base, path)
		if err != nil {
			return err
		}
	}
	parent := filepath.Dir(path)
	if parent == "." {
		parent = excludeTopDir
	}
	// an include parent will have all its parents in the
	// includes so again we only need to setup the
	// parent of last parent compoment
	// include parents follow exclude parents and so the excludeTopDir
	// record count would already be setup correctly
	exInfo.dirRecordCounts[parent]++
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

// processing the words involves:
//  a) setup the exclude and include RE based on the maps
//  b) setup the record counts based on the maps
//
// generating the regex at the end of adding all words
// helps in avoiding to edit the regex
func processWords(exInfo *ExcludeInfo, base string) error {
	var err error
	reWords := make([]string, 0)

	for word, _ := range exInfo.excludes {
		// Avoid dir10 regex to match dir103
		// by adding ^dir10$ and ^dir10/
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"$")
		// Adding trailing slash even for file excludes
		// is ok since file excludes will match it.
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"/")
		excludeSetRecordCount(exInfo, base, word)
	}
	exInfo.excludeRE, err = compileRE(reWords)
	if err != nil {
		return err
	}

	// Include paths which refer to a file or directory-without content
	// will have a dollar suffix in regex.
	// Include paths which refer to directory and its nested content will
	// have both dollar suffixed and slash suffixed entries in regex.
	reWords = make([]string, 0)
	for word, onlyDir := range exInfo.includes {
		includeSetRecordCount(exInfo, base, word)
		reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"$")
		if !onlyDir {
			reWords = append(reWords, "^"+regexp.QuoteMeta(word)+"/")
		}
	}
	exInfo.includeRE, err = compileRE(reWords)
	if err != nil {
		return err
	}
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

// returns the count of directory records that path should
// have
func (e *ExcludeInfo) RecordCount(path string, recs int) int {
	exrecs, exist := e.dirRecordCounts[path]
	if !exist {
		return recs
	}
	return exrecs
}

func LoadExcludeInfo(base string, filename string) (*ExcludeInfo, error) {

	var exInfo ExcludeInfo

	exInfo.dirRecordCounts = make(map[string]int)
	exInfo.excludes = make(map[string]struct{})
	exInfo.includes = make(map[string]bool)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	lineno := 0
	word := ""
	for s.Scan() {
		line := s.Text()
		line = strings.TrimSpace(line)
		lineno++
		// ignore comments and empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		// Word is obtained when line in exclude file passes
		// parsing (syntax checks). It retains special characters
		// like "+". Hence its neither a path nor a line.
		word, err = parseExcludeLine(base, line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, err)
		}

		err = checkExcludeRules(&exInfo, word)
		if err != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, err)
		}

		includePath := false
		if isIncludePath(word) {
			includePath = true
			word = strings.TrimPrefix(word, "+")
		}

		addWord(&exInfo, word, includePath)
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
