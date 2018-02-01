// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The timeseriesDb interface
package quantumfs

import (
	"time"
)

type Field struct {
	Name string
	Data interface{}
}

func NewFieldInt(name_ string, data_ int64) Field {
	return Field{
		Name: name_,
		Data: data_,
	}
}

func NewFieldString(name_ string, data_ string) Field {
	return Field{
		Name: name_,
		Data: data_,
	}
}

type Tag struct {
	Name string
	Data string
}

func NewTag(name_ string, data_ string) Tag {
	return Tag{
		Name: name_,
		Data: data_,
	}
}

type TimeSeriesDB interface {
	Store(measurement string, tags []Tag, fields []Field, t time.Time)
}
