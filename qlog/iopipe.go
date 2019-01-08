// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qlog

import "fmt"

func ioPipe(output *string) func(format string, args ...interface{}) error {
	return func(format string, args ...interface{}) error {
		newline := fmt.Sprintf(format, args...)
		*output += newline
		return nil
	}
}
