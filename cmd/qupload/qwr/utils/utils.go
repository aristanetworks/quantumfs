// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "bufio"
import "bytes"
import "fmt"
import "os"
import "path/filepath"
import "regexp"
import "strings"

// Given a bitflag field and an integer of flags, return whether the flags are set or
// not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}

type ExcludeInfo struct {
	re                  *regexp.Regexp
	dirRecordCounts     map[string]int
	rootDirRecordCounts int
}

func (e *ExcludeInfo) PathExcluded(path string) bool {
	result := e.re.MatchString(path)
	return result
}

func (e *ExcludeInfo) RecordsExcluded(path string, recs int) int {
	if path == "/" {
		return recs - e.rootDirRecordCounts
	}
	exrecs, exist := e.dirRecordCounts[path]
	if !exist {
		return recs
	}
	if exrecs == 0 {
		return 0
	}
	return recs - exrecs
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

func LoadExcludeInfo(base string, path string) (*ExcludeInfo, error) {

	var r bytes.Buffer
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

		// check to ensure the path entry in exlcude file is valid
		_, serr := os.Lstat(filepath.Join(base, word))
		if serr != nil {
			return nil, fmt.Errorf("%s:%d %v", path,
				lineno, serr)
		}

		r.WriteString("^" + word + "|")
		switch {
		case strings.HasSuffix(word, "/"):
			exInfo.dirRecordCounts[strings.TrimSuffix(word, "/")] = 0
		case filepath.Dir(word) == ".":
			exInfo.rootDirRecordCounts++
		default:
			exInfo.dirRecordCounts[filepath.Dir(word)]++
		}
		lineno++
	}

	if err = s.Err(); err != nil {
		return nil, err
	}

	reExp := strings.TrimSuffix(r.String(), "|")
	exInfo.re, err = regexp.Compile(reExp)
	if err != nil {
		return nil, err
	}
	return &exInfo, nil
}
