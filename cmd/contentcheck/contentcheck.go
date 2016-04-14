// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// contentcheck is a utility which confirms the source code is satisfactory to merge
// into master. This includes things like validating line length limitations.

package main

import "fmt"
import "io"
import "os"
import "path/filepath"
import "strings"

const maxLineLength = 85
const tabWidth = 8

var wasError = false

func main() {
	filepath.Walk(".", checkPath)

	if wasError {
		os.Exit(1)
	}
}

func printError(path string, line int, format string, args ...interface{}) {
	format = fmt.Sprintf("%s:%d: %s\n", path, line, format)
	fmt.Printf(format, args...)
}

func checkPath(path string, info os.FileInfo, err error) error {
	if info.IsDir() {
		return nil
	}

	// Walking failed reading this element, so we are unlikely going to be able
	// to read the file.
	if err != nil {
		return nil
	}

	if strings.HasSuffix(info.Name(), ".go") {
		if !checkGoFile(path, info) {
			wasError = false
		}
	}

	return nil
}

func checkGoFile(path string, info os.FileInfo) bool {
	file, err := os.Open(path)
	if err != nil {
		printError(path, 0, "Error opening file %v", err)
		return true
	}

	err = nil
	lineLength := 0
	lineNum := 1
	buf := make([]byte, 16384)

	for {
		bytesRead, err := file.Read(buf)

		if bytesRead == 0 && err == io.EOF {
			// We are done
			file.Close()
			return false
		}

		if err != nil {
			printError(path, lineNum, "Error reading file %v", err)
			return true
		}

		runes := string(buf)
		for _, glyph := range runes {
			switch glyph {
			default:
				lineLength++
			case '\n':
				if lineLength > maxLineLength {
					printError(path, lineNum, "Line length"+
						" exceeds coding style maximum")
				}
				lineNum++
				lineLength = 0
			case '\t':
				lineLength += tabWidth
			}
		}
	}

	file.Close()
	return false
}
