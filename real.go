// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/gocql/gocql"
)

// RealCluster is a wrapper around gocql.ClusterConfig
type RealCluster struct {
	cluster *gocql.ClusterConfig
}

// NewRealCluster returns a default Cluster struct with the given hosts
func NewRealCluster(clusterCfg ClusterConfig) Cluster {

	var err error
	nodes := clusterCfg.Nodes
	if clusterCfg.ClusterName != "" {
		nodes, err = net.LookupHost(clusterCfg.ClusterName)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"** WARNING: LookupHost failed for %s . Using node IPs from config **\n", clusterCfg.ClusterName)
			nodes = clusterCfg.Nodes
		}
	}
	c := gocql.NewCluster(nodes...)
	c.ProtoVersion = 3
	c.Consistency = gocql.Quorum
	// ConsistencyLevel CL is an undocumented/developer-only/internal
	// setting hence it is not provided in ClusterConfig
	cl := os.Getenv("CQL_CL")
	if cl != "" {
		fmt.Fprintf(os.Stderr, "** WARNING: CQL_CL=%s **\n", cl)
		// ParseConsistency will panic if illegal values are used
		c.Consistency = gocql.ParseConsistency(cl)
	}
	c.PoolConfig.HostSelectionPolicy =
		gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	c.Events.DisableSchemaEvents = true

	if clusterCfg.Username != "" && clusterCfg.Password != "" {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: clusterCfg.Username,
			Password: clusterCfg.Password,
		}
	} else {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: scyllaUsername,
			Password: scyllaPassword,
		}
	}

	if clusterCfg.QueryNumRetries == 0 {
		clusterCfg.QueryNumRetries = 2
	}

	if clusterCfg.CheckSchemaRetries == 0 {
		clusterCfg.CheckSchemaRetries = 2
	}

	c.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: clusterCfg.QueryNumRetries}

	if clusterCfg.ConnTimeoutSec == 0 {
		clusterCfg.ConnTimeoutSec = 3
	}

	c.Timeout = time.Duration(clusterCfg.ConnTimeoutSec) * time.Second

	cc := &RealCluster{
		cluster: c,
	}

	return cc
}

// CreateSession initiates a session with the cql cluster
// and returns a session object
func (c *RealCluster) CreateSession() (Session, error) {
	s, err := c.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	ss := &RealSession{
		session: s,
	}
	return ss, err
}

// RealSession is a wrapper around gocql.Session
type RealSession struct {
	session *gocql.Session
}

// Close closes the session with the cql cluster
func (s *RealSession) Close() {
	s.session.Close()
}

// Closed returns true if the session with the cluster is closed
func (s *RealSession) Closed() bool {
	return s.session.Closed()
}

// Query returns a Query object for the given <stmt, values>
func (s *RealSession) Query(stmt string, values ...interface{}) Query {
	q := s.session.Query(stmt, values...)
	// NOTE: Since s.session.Query() returns a struct, it's ok to compare to nil.
	if q == nil {
		return nil
	}
	qq := &RealQuery{
		query: q,
	}
	return qq
}

// RealQuery is a wrapper around gocql.Query
type RealQuery struct {
	query *gocql.Query
}

// Exec executes the given query
func (q *RealQuery) Exec() error {
	return q.query.Exec()
}

// Scan executes the query, copies the columns of the first
// selected row into the values pointed at by dest and discards
// the rest. If no rows were selected, ErrNotFound is returned
func (q *RealQuery) Scan(dest ...interface{}) error {
	return q.query.Scan(dest...)
}

// String implements the stringer interface for RealQuery
func (q *RealQuery) String() string {
	return q.query.String()
}

// Iter returns iter for RealQuery
func (q *RealQuery) Iter() Iter {
	i := q.query.Iter()
	// NOTE: Since q.query.Iter() returns a struct, it's ok to compare to nil.
	if i == nil {
		return nil
	}

	ii := &RealIter{
		iter: i,
	}
	return ii
}

// RealIter is a wrapper around gocql.Iter
type RealIter struct {
	iter *gocql.Iter
}

// Close is a wrapper around gocql.Iter.Close()
func (i *RealIter) Close() error {
	return i.iter.Close()
}

// GetCustomPayload is a wrapper around gocql.Iter.GetCustomPayload()
func (i *RealIter) GetCustomPayload() map[string][]byte {
	return i.iter.GetCustomPayload()
}

// MapScan is a wrapper around gocql.Iter.MapScan()
func (i *RealIter) MapScan(m map[string]interface{}) bool {
	return i.iter.MapScan(m)
}

// NumRows is a wrapper around gocql.Iter.NumRows()
func (i *RealIter) NumRows() int {
	return i.iter.NumRows()
}

// PageState is a wrapper around gocql.Iter.PageState()
func (i *RealIter) PageState() []byte {
	return i.iter.PageState()
}

// Scan is a wrapper around gocql.Iter.Scan()
func (i *RealIter) Scan(dest ...interface{}) bool {
	return i.iter.Scan(dest...)
}

// SliceMap is a wrapper around gocql.Iter.SliceMap()
func (i *RealIter) SliceMap() ([]map[string]interface{}, error) {
	return i.iter.SliceMap()
}

// WillSwitchPage is a wrapper around gocql.Iter.WillSwitchPage()
func (i *RealIter) WillSwitchPage() bool {
	return i.iter.WillSwitchPage()
}
