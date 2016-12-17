// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// Cluster is a interface to configure the default cluster implementation.
// Cluster will be implemented by real and mock gocql.
type Cluster interface {
	CreateSession() (Session, error)
}

// Session is the interface used by users to interact with the database.
// Session will be implemented by real and mock gocql.
type Session interface {
	Close()
	Closed() bool
	Query(stmt string, values ...interface{}) Query
}

// Query represents a CQL statement that can be executed.
// Query will be implemented by real and mock gocql.
type Query interface {
	Exec() error
	Scan(dest ...interface{}) error
	String() string
	Iter() Iter
}

// Iter represents an iterator that can be used to iterate over all rows that
// were returned by a query.
// Iter will be implemented by real and mock gocql.
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
