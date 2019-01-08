// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

// Idea from  https://play.golang.org/p/tkE2lH6AlQ
//
// Use:
//
// type foo struct {
// 	_ utils.Uncomparable
// 	...
// }
//
// Causes the type foo to become uncomparable, using the operator == on a variable of
// type foo will cause a compiler error.
type Uncomparable [0]func()
