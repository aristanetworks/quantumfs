// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "bufio"
import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "path/filepath"
import "regexp"
import "strings"

const excludeTopDir = "/"

type ExcludeInfo struct {
	// temporary maps to collect exclude and include paths
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
	fmt.Printf("PathExcluded(%s) excl:%v incl:%v\n", path, excl, incl)
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

func parseExcludeLine(line string) (string, error) {
	parts := strings.Split(line, " ")
	// rules
	switch {
	case len(parts) > 1:
		return "", fmt.Errorf("whitespace in a line")
	case strings.HasPrefix(line, "/"):
		return "", fmt.Errorf("path has / prefix")
	case strings.HasPrefix(line, "."):
		return "", fmt.Errorf("path has . prefix")
	case !strings.HasPrefix(line, "+") && strings.HasSuffix(line, "/"):
		return "", fmt.Errorf("exclude path has / suffix")
	}
	return parts[0], nil
}

func checkExcludeRules(exInfo *ExcludeInfo, word string) error {
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

func addWord(exInfo *ExcludeInfo, word string, isIncludePath bool) {
	if !isIncludePath {
		exInfo.excludes[word] = true
	} else {
		exInfo.includes[word] = true
	}
}

func decRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	if path == "." {
		path = excludeTopDir
	}
	err := initRecordCount(exInfo, base, path)
	if err != nil {
		return err
	}
	exInfo.dirRecordCounts[path]--
	fmt.Printf("decRecordCount(%s) = %d\n",
		path,
		exInfo.dirRecordCounts[path])
	return nil
}

func incRecordCount(exInfo *ExcludeInfo, base string, path string) error {
	if path == "." {
		path = excludeTopDir
	}
	// no need to initRecordCount since the record count
	// for dirs in include path is completely based on
	// includeMap and is independent of the record count
	// in the source directory (base + path).
	// TODO: add comment about excludeTopDir record count
	exInfo.dirRecordCounts[path]++
	fmt.Printf("incRecordCount(%s) = %d\n",
		path,
		exInfo.dirRecordCounts[path])
	return nil
}

// processing the words involves:
//  a) setup the exclude and include RE based on the maps
//  b) setup the record counts based on the maps
func processWords(exInfo *ExcludeInfo, base string) error {
	var exRE bytes.Buffer
	var inRE bytes.Buffer
	var err error

	for word, _ := range exInfo.excludes {
		exRE.WriteString("^" + word + "|")

		// an exclude path only says that the last path-component
		// is excluded and hence only its parent's record count is
		// impacted, grand-parents remain unaffected by this path
		decRecordCount(exInfo, base, filepath.Dir(word))
	}
	re := strings.TrimSuffix(exRE.String(), "|")
	fmt.Println("ExcludeRE = ", re)
	exInfo.excludeRE, err = regexp.Compile(re)
	if err != nil {
		return err
	}

	for word, _ := range exInfo.includes {
		inRE.WriteString("^" + word + "$|")

		// an include path will have all its parents in the
		// includes so again we only need to setup the
		// parent of last path compoment
		//
		// since we allow suffix of "/" in include paths
		// trim it for record counting purposes
		incRecordCount(exInfo, base,
			filepath.Dir(strings.TrimSuffix(word, "/")))
	}
	re = strings.TrimSuffix(inRE.String(), "|")
	if re != "" {
		fmt.Println("IncludeRE = ", re)
		exInfo.includeRE, err = regexp.Compile(re)
		if err != nil {
			return err
		}
	}

	fmt.Printf("RecordCounts: %v\n", exInfo.dirRecordCounts)
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
	lineno := 1
	word := ""
	for s.Scan() {
		line := s.Text()
		line = strings.TrimSpace(line)
		// ignore comments and empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		word, err = parseExcludeLine(line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, err)
		}

		err = checkExcludeRules(&exInfo, word)
		if err != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, err)
		}

		isIncludePath := false
		if strings.HasPrefix(word, "+") {
			isIncludePath = true
			word = strings.TrimPrefix(word, "+")
		}
		// check to ensure the path entry in exlcude file is valid
		_, serr := os.Lstat(filepath.Join(base, word))
		if serr != nil {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %v",
				filename, lineno, serr)
		}

		addWord(&exInfo, word, isIncludePath)
		lineno++
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
