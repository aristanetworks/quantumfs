// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "bufio"
import "fmt"
import "os"
import "strings"

// Given a bitflag field and an integer of flags, return whether the flags are set or
// not as a boolean.
func BitFlagsSet(field uint, flags uint) bool {
	if field&flags == flags {
		return true
	}
	return false
}

var excludeLines []string

func parseExcludeLine(line string) bool {
	line = strings.TrimSpace(line)
	// ignore comments and empty lines
	if len(line) == 0 || strings.HasPrefix(line, "#") {
		return true
	}

	// consider one word per line
	parts := strings.Split(line, " ")
	//fmt.Println("Excluding ", parts[0])
	excludeLines = append(excludeLines, parts[0])
	return true
}

func IsPathExcluded(path string) bool {
	for _, exPath := range excludeLines {
		//fmt.Println("Ex: ", exPath, "In: ", path)
		if strings.HasPrefix(path, exPath) {
			return true
		}
	}
	return false
}

func LoadExcludeList(path string) error {
	file, oerr := os.Open(path)
	if oerr != nil {
		return oerr
	}
	defer file.Close()

	s := bufio.NewScanner(file)
	lineno := 1
	for s.Scan() {
		line := s.Text()
		if !parseExcludeLine(line) {
			return fmt.Errorf("Bad exclude line: %s at: %d\n",
				line, lineno)
		}
		lineno++
	}

	if serr := s.Err(); serr != nil {
		return serr
	}
	return nil
}
