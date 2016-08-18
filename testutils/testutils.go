// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package testutils

import "fmt"

func IoPipe(output *string) func(format string, args ...interface{}) error {
	return func(format string, args ...interface{}) error {
		newline := fmt.Sprintf(format, args...)
		*output += newline
		return nil
	}
}
