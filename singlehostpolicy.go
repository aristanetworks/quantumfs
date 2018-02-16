// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

// This HostSelectionPolicy supports max of 1 host.
// It supports the usecase where a failed query with non-nil retry policy
// should be retried on the single host as long as the host is present and up.
//
// This policy is useful in test environments like dockerized single-instance
// Scylla where system load may cause a query to timeout/error but retrying it
// may work fine.
//
// NOTE: Currently the policy cannot distinguish between retryable and
//       non-retryable error and so non-retryable errors will unnecessarily
//       be retried.
//
// None of the HostSelectionPolicies in GoCQL support this usecase of
// retrying a failed query on the same host
import (
	"github.com/gocql/gocql"
)

type singleHostPolicy struct {
	gocql.HostSelectionPolicy

	host   *gocql.HostInfo
	hostUp bool
}

func newSingleHostPolicy() gocql.HostSelectionPolicy {
	return &singleHostPolicy{}
}

func (s *singleHostPolicy) AddHost(h *gocql.HostInfo) {
	s.host = h
	s.hostUp = true
}
func (s *singleHostPolicy) RemoveHost(h string) {
	s.host = nil
	s.hostUp = false
}
func (s *singleHostPolicy) HostUp(h *gocql.HostInfo) {
	s.AddHost(h)
}
func (s *singleHostPolicy) HostDown(h string) {
	s.RemoveHost(h)
}

func (s *singleHostPolicy) SetPartitioner(string) {}

// hi implements gocql.SelectedHost
type hi gocql.HostInfo

func (host *hi) Info() *gocql.HostInfo { return (*gocql.HostInfo)(host) }
func (host *hi) Mark(err error)        {}

func (s *singleHostPolicy) Pick(gocql.ExecutableQuery) gocql.NextHost {
	return func() gocql.SelectedHost {
		if s.host == nil || !s.hostUp {
			return nil
		}
		return (*hi)(s.host)
	}
}
