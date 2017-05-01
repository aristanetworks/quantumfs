// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "bufio"
import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "regexp"
import "strings"

type ExcludeInfo struct {
	re              *regexp.Regexp // paths excluded
	override        *regexp.Regexp // paths not-excluded
	dirRecordCounts map[string]int
}

func (e *ExcludeInfo) PathExcluded(path string) bool {
	excl := e.re.MatchString(path)
	incl := e.override.MatchString(path)
	return excl && !incl
}

// returns the count of directory records that path should
// have
func (e *ExcludeInfo) RecordCount(path string, recs int) int {
	exrecs, exist := e.dirRecordCounts[path]
	if !exist {
		return recs
	}
	// 0 is a special case to indicate that all records
	// in this path should be excluded
	if exrecs == 0 {
		return 0
	}
	return exrecs
}

func parseExcludeLine(line string) (string, bool) {
	parts := strings.Split(line, " ")
	// rules
	switch {
	// only 1 word per line
	case len(parts) > 1:
		return "", false
	// word must not begin with /
	case strings.HasPrefix(line, "/"):
		return "", false
	// word must not begin with .
	case strings.HasPrefix(line, "."):
		return "", false
	}
	return parts[0], true
}

// if the path is an exclude path then setup the exclude regex appropriately
// if the path is an include path (or exclude override) then setup
//  override regexp accordingly
func handlePath(exInfo *ExcludeInfo, exclude bytes.Buffer, include bytes.Buffer,
	word string, isIncludePath bool) {

	if !isIncludePath {
		exclude.WriteString("^" + word + "|")

		switch {
		case strings.HasSuffix(word, "/"):
			exInfo.dirRecordCounts[strings.TrimSuffix(word, "/")] = 0
		case filepath.Dir(word) == ".":
			rootDirRecords--
		default:
			_, ok := exInfo.dirRecordCounts[filepath.Dir(word)]
			if !ok {

			}
			exInfo.dirRecordCounts[filepath.Dir(word)]--
		}

	}

}

func LoadExcludeInfo(base string, path string) (*ExcludeInfo, error) {

	var exclude bytes.Buffer
	var include bytes.Buffer
	var exInfo ExcludeInfo

	exInfo.dirRecordCounts = make(map[string]int)
	exInfo.includePaths = make(map[string]bool)

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	lineno := 1
	for s.Scan() {
		line := s.Text()
		line = strings.TrimSpace(line)
		// ignore comments and empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		word, ok := parseExcludeLine(line)
		if !ok {
			return nil, fmt.Errorf("%s:%d Bad exclude line: %s",
				path, lineno, line)
		}

		isIncludePath := false
		// i.e. these paths should not be excluded
		if strings.HasPrefix(word, "+") {
			isIncludePath = true
			word = strings.TrimPrefix(word, "+")
			exInfo.includePaths[word] = true
		}

		// check to ensure the path entry in exlcude file is valid
		_, serr := os.Lstat(filepath.Join(base, word))
		if serr != nil {
			return nil, fmt.Errorf("%s:%d %v", path,
				lineno, serr)
		}

		handlePath(include, exclude, word, isIncludePath)

		lineno++
	}

	if err = s.Err(); err != nil {
		return nil, err
	}

	reExp := strings.TrimSuffix(exclude.String(), "|")
	exInfo.re, err = regexp.Compile(reExp)
	if err != nil {
		return nil, err
	}
	reExp = strings.TrimSuffix(include.String(), "|")
	exInfo.override, err = regexp.Compile(reExp)
	if err != nil {
		return nil, err
	}
	return &exInfo, nil
}
