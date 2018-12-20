// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Package cql implements the blobstore, workspacedb and blobmap APIs
// using gocql to talk to the gocql library
//
// The package creates interfaces of structs from the gocql
// library. We have only picked up the structs and their  methods
// that we think we will need viz. Cluster, Session, Query and Iter.
//
// There are 2 implementations of the interface below:
// 1. real.go: A wrapper around gocql library.
// 2. mock.go: A mock implmentation of this interface using testify/mock.
//
// Both of these implementations use constants and error codes from the
// gocql library.
package cql
