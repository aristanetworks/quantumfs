// run as scylla superuser
CONSISTENCY ALL;

CREATE KEYSPACE IF NOT EXISTS cqlKS
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
CREATE TABLE IF NOT EXISTS cqlKS.blobstore ( key blob PRIMARY KEY, value blob );

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
