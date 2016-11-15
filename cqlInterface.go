// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This file creates interfaces of structs from the gocql
// library. We have only picked up the structs and their  methods
// that we think we will need.
//
// There are 2 implementations of the interface below:
// 1. cqlReal.go: A wrapper around gocql library.
// 2. cqlMock.go: A mock implmentation of this interface using testify/mock.
//
// Both of these implementations use constants and error codes from the
// gocql library.
package cql

// Cluster will be implemented by real and mock gocql
type Cluster interface {
	CreateSession() (Session, error)
}

// Session will be implemented by real and mock gocql
type Session interface {
	Close()
	Closed() bool
	Query(stmt string, values ...interface{}) Query
}

// Query will be implemented by real and mock gocql
type Query interface {
	Exec() error
	Scan(dest ...interface{}) error
	String() string
	Iter() Iter
}

// Iter will be implemented by real and mock gocql
type Iter interface {
	Close() error
	GetCustomPayload() map[string][]byte
	MapScan(m map[string]interface{}) bool
	NumRows() int
	PageState() []byte
	Scan(dest ...interface{}) bool
	SliceMap() ([]map[string]interface{}, error)
	WillSwitchPage() bool
}
