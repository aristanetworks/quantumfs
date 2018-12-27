// run as scylla superuser
CONSISTENCY ALL;

CREATE KEYSPACE IF NOT EXISTS cqlKS
  WITH REPLICATION = { 'class':'NetworkTopologyStrategy','DC1':'3'};
CREATE TABLE IF NOT EXISTS cqlKS.blobstore ( key blob PRIMARY KEY, value blob )
  WITH compaction = { 'class': 'LeveledCompactionStrategy' };

CREATE KEYSPACE IF NOT EXISTS cqlKSwsdb
  WITH REPLICATION = { 'class':'NetworkTopologyStrategy','DC1':'3'};

-- workspacedb table is created with gc_grace_seconds of 12 hours.
-- SizeTieredCompactionStrategy is used due to frequent repairs and compactions.

CREATE TABLE IF NOT EXISTS cqlKSwsdb.workspacedb
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


create user if not exists usrCqlIntegTest with password 'usrCqlIntegTest' nosuperuser;
revoke all permissions on all keyspaces from usrCqlIntegTest;

grant modify on keyspace cqlKS to usrCqlIntegTest;
grant select on keyspace cqlKS to usrCqlIntegTest;
grant modify on keyspace cqlKSwsdb to usrCqlIntegTest;
grant select on keyspace cqlKSwsdb to usrCqlIntegTest;
grant select on system_auth.permissions to usrCqlIntegTest;

grant create on keyspace cqlKS to usrCqlIntegTest;
grant drop on keyspace cqlKS to usrCqlIntegTest;
grant create on keyspace cqlKSwsdb to usrCqlIntegTest;
grant drop on keyspace cqlKSwsdb to usrCqlIntegTest;
