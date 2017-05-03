// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "bufio"
import "fmt"
import "io/ioutil"
import "os"
import "path/filepath"
import "regexp"
import "strings"

const excludeTopDir = "/"

type ExcludeInfo struct {
	// temporary maps to collect exclude and include paths
	// boolean value indicates if regex must use dollar or not
	excludes map[string]bool
	includes map[string]bool

	excludeRE *regexp.Regexp // exclude path regexp
	includeRE *regexp.Regexp // include path regexp

	// desired count of records for a path
	dirRecordCounts map[string]int
}

func (e *ExcludeInfo) PathExcluded(path string) bool {
	// a path is excluded if theres a match in excludeRE
	// and no match in includeRE
	// if a path doesn't match excludeRE then no need to check
	// includeRE
	excl := e.excludeRE.MatchString(path)
	incl := false
	if excl && e.includeRE != nil {
		incl = e.includeRE.MatchString(path)
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

func isIncludePath(word string) bool {
	return strings.HasPrefix(word, "+")
}

func parseExcludeLine(base string, line string) (string, error) {
	parts := strings.Split(line, " ")
	// rules
	switch {
	case len(parts) > 1:
		return "", fmt.Errorf("whitespace in a line")
	case strings.HasPrefix(line, "/"):
		return "", fmt.Errorf("path has / prefix")
	case strings.HasPrefix(line, "."):
		return "", fmt.Errorf("path has . prefix")
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
	_, ok := exInfo.dirRecordCounts[path]
	if !ok {
		dirEnts, dirErr := ioutil.ReadDir(filepath.Join(dir, path))
		if dirErr != nil {
			return fmt.Errorf("ReadDir failed for %s error: %v",
				dir, dirErr)
		}
		exInfo.dirRecordCounts[path] = len(dirEnts)
	}
	return nil
}

func addWord(exInfo *ExcludeInfo, word string, includePath bool) {
	if !includePath {
		exInfo.excludes[word] = true
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
	err := initRecordCount(exInfo, base, parent)
	if err != nil {
		return err
	}
	exInfo.dirRecordCounts[parent]--
	return nil
}

func includeSetRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	if exInfo.includes[path] {
		// if only the directory name is being included then setup the
		// record count
		exInfo.dirRecordCounts[path] = 0
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

// processing the words involves:
//  a) setup the exclude and include RE based on the maps
//  b) setup the record counts based on the maps
//
// generating the regex at the end of adding all words
// helps in avoiding to edit the regex
func processWords(exInfo *ExcludeInfo, base string) error {
	var err error
	exRE := make([]string, 0)
	inRE := make([]string, 0)

	for word, _ := range exInfo.excludes {
		exRE = append(exRE, "^"+regexp.QuoteMeta(word))
		excludeSetRecordCount(exInfo, base, word)
	}
	re := strings.Join(exRE, "|")
	exInfo.excludeRE, err = regexp.Compile(re)
	if err != nil {
		return err
	}

	for word, useDollar := range exInfo.includes {
		includeSetRecordCount(exInfo, base, word)
		if !useDollar {
			inRE = append(inRE, "^"+regexp.QuoteMeta(word))
			continue
		}
		inRE = append(inRE, "^"+regexp.QuoteMeta(word)+"$")
	}
	re = strings.Join(inRE, "|")
	if re != "" {
		exInfo.includeRE, err = regexp.Compile(re)
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadExcludeInfo(base string, filename string) (*ExcludeInfo, error) {

	var exInfo ExcludeInfo

	exInfo.dirRecordCounts = make(map[string]int)
	exInfo.excludes = make(map[string]bool)
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

func (exInfo *ExcludeInfo) DumpState() {
	fmt.Println("Dump of exclude information")
	fmt.Printf("%v\n", exInfo)
	if exInfo.excludeRE != nil {
		fmt.Printf("Exclude Regex: %s\n", exInfo.excludeRE.String())
	} else {
		fmt.Printf("Exclude Regex is nil\n")
	}
	if exInfo.includeRE != nil {
		fmt.Printf("Include Regex: %s\n", exInfo.includeRE.String())
	} else {
		fmt.Printf("Include Regex is nil\n")
	}
}
