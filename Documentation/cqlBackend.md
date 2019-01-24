# CQL Backend

Package `github.com/aristanetworks/quantumfs/backends/cql` provides
a backend to store quantumfs’ data blocks and workspaceDB info in a CQL
compatible backend. It has verified to work with ScyllaDB and Apache-Cassandra.

In order to use the cql backend the CQL database needs to be setup with the
following schema:

```
CONSISTENCY ALL;
-- Keyspace: cqlKS, Table: blobstore, store data blocks. Keyspace name
-- can be different, but the table name needs to be blobstore.
-- The 'replication_factor' should be changed to whatever deemed appropriate by the
-- cluster admins, we recommend a 'replication_factor' of 3.
CREATE KEYSPACE IF NOT EXISTS cqlKS
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS cqlKS.blobstore ( key blob PRIMARY KEY, value blob );

-- Keyspace: cqlKSwsdb, Table: workspacedb, store wsdb info. Keyspace name can be
-- different, but the table name needs to be workspacedb.
-- The 'replication_factor' should be changed to whatever deemed appropriate by the
-- cluster admins, we recommend a 'replication_factor' of 3.
CREATE KEYSPACE IF NOT EXISTS cqlKSwsdb
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS cqlKSwsdb.workspacedb
  ( typespace text,
    namespace text,
    workspace text,
    key blob,
    nonce bigint,
    publishtime bigint,
    immutable boolean,
    PRIMARY KEY ( typespace, namespace, workspace ));
```

In order to start quantumfs with a CQL backend, you need a CQL compatible database
and a config file. The config file contains information such as the name of the 2
keyspaces and hostnames for the servers running the database.

A sample config file is present here:
```
backends/cql/cluster_configs/smoke_test_config
```

## Garbage Collection
QuantumFS does not maintain reference counts for its blocks. Space is reclaimed on
the database by the way of Time-To-Live(TTL) based garbage collection. So, in order
to prevent blocks from getting GCed due to an expiring TTL, we need to periodically
refresh the TTL values of the blocks in the database we want to keep.

The config value states 3 TTL values as below:
```
"adapter":
{
  "ttlrefreshtime" : "72h",   // If the TTL is less than this, we do not refresh TTL.
  "ttlrefreshvalue": "192h",  // This is the TTL for a new block.
  "ttldefaultvalue": "192h"   // The is the TTL value when a TTL value is refreshed.
}
```

The cqlwalkerd service in walks all the workspaces in quantumfs and refreshes their
TTL based on the values above.

