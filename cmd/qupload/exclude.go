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

type ExcludeInfo struct {
	re              *regexp.Regexp // paths excluded
	override        *regexp.Regexp // paths not-excluded
	dirRecordCounts map[string]int
}

func (e *ExcludeInfo) PathExcluded(path string) bool {
	excl := e.re.MatchString(path)
	if e.override != nil {
		excl = excl && !e.override.MatchString(path)
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
func handlePath(exInfo *ExcludeInfo, exclude *bytes.Buffer, include *bytes.Buffer,
	base string, word string, isIncludePath bool) error {

	_, ok := exInfo.dirRecordCounts["/"]
	if !ok {
		dirEnts, dirErr := ioutil.ReadDir(base)
		if dirErr != nil {
			return fmt.Errorf("Loading exclude info error: %v",
				dirErr)
		}
		exInfo.dirRecordCounts["/"] = len(dirEnts)
	}

	if !isIncludePath {
		exclude.WriteString("^" + word + "|")

		switch {
		case strings.HasSuffix(word, "/"):
			exInfo.dirRecordCounts[strings.TrimSuffix(word, "/")] = 0
		case filepath.Dir(word) == ".":
			exInfo.dirRecordCounts["/"]--
		default:
			_, ok := exInfo.dirRecordCounts[filepath.Dir(word)]
			if !ok {
				dirEnts, dirErr := ioutil.ReadDir(
					filepath.Join(base, filepath.Dir(word)))
				if dirErr != nil {
					return fmt.Errorf("Loading exclude "+
						"info error: %v", dirErr)
				}
				exInfo.dirRecordCounts[filepath.Dir(word)] =
					len(dirEnts)
			}
			exInfo.dirRecordCounts[filepath.Dir(word)]--
		}
	} else {
		include.WriteString("^" + word + "|")
		if filepath.Dir(word) == "." {
			exInfo.dirRecordCounts["/"]++
		} else {
			exInfo.dirRecordCounts[filepath.Dir(word)]++
		}
	}

	return nil
}

func LoadExcludeInfo(base string, path string) (*ExcludeInfo, error) {

	var exclude bytes.Buffer
	var include bytes.Buffer
	var exInfo ExcludeInfo

	exInfo.dirRecordCounts = make(map[string]int)

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
		}

		// check to ensure the path entry in exlcude file is valid
		_, serr := os.Lstat(filepath.Join(base, word))
		if serr != nil {
			return nil, fmt.Errorf("%s:%d %v", path,
				lineno, serr)
		}

		err = handlePath(&exInfo, &exclude, &include, base, word,
			isIncludePath)
		if err != nil {
			return nil, err
		}
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
	if include.String() != "" {
		reExp = strings.TrimSuffix(include.String(), "|")
		exInfo.override, err = regexp.Compile(reExp)
		if err != nil {
			return nil, err
		}
	}

	return &exInfo, nil
}
