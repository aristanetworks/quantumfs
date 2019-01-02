// run as scylla superuser
CONSISTENCY ALL;

CREATE KEYSPACE IF NOT EXISTS dev_EtherIntegTest
  WITH REPLICATION = { 'class':'NetworkTopologyStrategy','DC1':'3'};
CREATE TABLE IF NOT EXISTS dev_EtherIntegTest.blobstore ( key blob PRIMARY KEY, value blob )
  WITH compaction = { 'class': 'LeveledCompactionStrategy' };

CREATE KEYSPACE IF NOT EXISTS dev_EtherIntegTestwsdb
  WITH REPLICATION = { 'class':'NetworkTopologyStrategy','DC1':'3'};

-- workspacedb table is created with gc_grace_seconds of 12 hours.
-- SizeTieredCompactionStrategy is used due to frequent repairs and compactions.

CREATE TABLE IF NOT EXISTS dev_EtherIntegTestwsdb.workspacedb
  ( typespace text,
    namespace text,
    workspace text,
    key blob,
    nonce bigint,
    publishtime bigint,
    immutable boolean,
    PRIMARY KEY ( typespace, namespace, workspace ))
  WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }
  AND gc_grace_seconds = 43200;


create user if not exists usrEtherIntegTest with password 'usrEtherIntegTest' nosuperuser;
revoke all permissions on all keyspaces from usrEtherIntegTest;

grant modify on keyspace dev_EtherIntegTest to usrEtherIntegTest;
grant select on keyspace dev_EtherIntegTest to usrEtherIntegTest;
grant modify on keyspace dev_EtherIntegTestwsdb to usrEtherIntegTest;
grant select on keyspace dev_EtherIntegTestwsdb to usrEtherIntegTest;
grant select on system_auth.permissions to usrEtherIntegTest;

grant create on keyspace dev_EtherIntegTest to usrEtherIntegTest;
grant drop on keyspace dev_EtherIntegTest to usrEtherIntegTest;
grant create on keyspace dev_EtherIntegTestwsdb to usrEtherIntegTest;
grant drop on keyspace dev_EtherIntegTestwsdb to usrEtherIntegTest;