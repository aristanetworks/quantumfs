# What QuantumFS is

QuantumFS is a distributed FUSE filesystem optimized for large-scale
software development use-cases. That's a dense statement with many many
implications which we'll cover in the respective sections below.

QuantumFS is built to be used by large teams with large, fast moving code bases.
It contains several novel features designed to make building software in the
large easier.

## Distributed

The highest level architecture diagram of QuantumFS looks like so:

                       +-------------+
                       |             |
                  _____|    WSDB     |_______
     +---------+ /     |             |       \ +---------+
     |         |/      +-------------+        \|         |
     | Server0 |\                             /| Server1 |
     |         | \     +-------------+       / |         |
     +---------+  \    |             |\------  +---------+
                   ----|  Datastore  ||\
                       |             |||\
                       +-------------+|||
                        \-------------+||
                         \-------------+|
                          +-------------+

Where we see three major class of systems. At the top we have WSDB, which stands
for WorkSpace DataBase. This component is the central synchronization point of
the system which maintains a mapping from workspace names to root IDs. One good
way to think of this is that the WSDB holds the global list of branches and the
commit IDs they refer to.

Below the WSDB there is the Datastore. This can be any suitably fast and durable
key-value store. Every file object (directory, regular file, extended attribute,
etc.) is split into a number of blocks with a fixed, moderate maximum size.
These blocks are stored in the datastore for persistence. Blocks are addressed
by their contents and never modified after being written.

Finally, we have a number of servers. These servers are where QuantumFS is
mounted as a filesystem for use. QuantumFS instances do not communicate directly
with each other. Instead all inter-system signalling occurs via either the WSDB
or the datastore.

The strict limits on communication and minimal semantic demands on the datastore
give QuantumFS good horizontal scaling attributes.

It is quite possible to run QuantumFS on a single machine with all three
components co-located. Doing so will provide many of the advantages of
QuantumFS, but obviously will not provide any horizontal scaling.

## FUSE filesystem

QuantumFS primarily presents a nearly POSIX compliant filesystem to the local
system using FUSE. Currently only Linux is supported.

This means all the normal development tools continue to work unmodified.

QuantumFS also provides some features beyond what can be provided using a
filesystem API using the qfs command.

## Optimized for large-scale software development

Software development makes different demands on a filesystem than other uses and
scale causes problems of its own. QuantumFS is optimized to fill the needs of
software development and takes advantage of eliminating optional features when
prudent.

This is evident in three major ways and several minor ways. First the major
implications which are critical to understand.

1. Consistency. Developers require all the local consistency of a normal
   filesystem to ensure that normal tools continue to operate as expected.
   However, developers don't often use multiple systems to read the same
   workspaces concurrently. QuantumFS strives to provide strict consistency
   within a single instance, normally one machine. That is, locally QuantumFS
   should be no less consistent than a locally mounted disk-based filesystem.

   Consistency between QuantumFS instances, however, do not provide strict
   consistency at all. Instead they provide plausible eventual consistency on
   human timescales, seconds or minutes. A quick way to conceptualize this is to
   consider two developers working on different QuantumFS mounts within the same
   workspace. Suppose each developer makes a change to the workspace, then walks
   to the coffee machine and tells the other what they did. If they checked the
   state of the workspace at the coffee machine they would see the combined
   result of both changes in an order which could have happened because the two
   developers were not closing communicating when they made the change. Note
   that the result may not match the expected outcome if the two modifications
   were considered in chronological order; it's plausible the combination would
   have only occurred if the original modifications occurred in a modified time
   order.

2. Durability. Most of the valuable output of a developer is the source code
   they produce. This is stored in a VCS, backed up and will survive the
   destruction of the developer's system. Everything else can be reproduced with
   more or less cost from what is in source control. Consequently QuantumFS
   plays loose with data durability in the interest of speed. fsync() and
   friends are ignored. Unless otherwise prompted QuantumFS flushes its dirty
   data to the datastore in a leisurely manner. Until the local data has been
   flushed it isn't available to other instances. Don't use QuantumFS for a
   critical database, it will eventually eat your data.

   Certain qfs commands will cause a workspace to be synchronously flushed when
   that is necessary.

3. Branching workspaces. One major problem with large-scale software development
   are the large code bases with large build outputs. At a certain scale merely
   changing the build products to match the source code becomes too slow for
   optimal developer productivity as they switch between tasks. Just like source
   code, branching workspaces allows one to quickly and cheaply work on multiple
   tasks.

   Branching can also be used to ensure isolated build environments for build
   tasks, possibly even distributed across a cluster.

These major implications make QuantumFS unsuitable for many uses which aren't
software development.

The minor implications are several and only the most important are listed here:

1. Magic ownership. Large teams are large and large teams work on mostly the
   same things. In order to ease filesystem churn and permission issues
   QuantumFS uses magic permissions. Outside a limited number of system IDs, all
   filesystem objects are automatically owned by the currently accessing user.
   That is, DeveloperA will see a file as owned by their user account.
   DeveloperB will see the same thing. In fact, both will see the same file in
   the same workspace at the same time as owned by themselves.

2. The plausible eventual consistency model assumes humans are making the
   modifications. The resolutions may be unexpected for sequences of operations
   which don't follow human behaviour.

3. QuantumFS tracks which files you touch. The accessed file list tracks, per
   instance per workspace, which files are read, modified, created or deleted.
   This is useful for build tools to track dependencies.

# What QuantumFS is not

Though QuantumFS strives to provide local consistency and POSIX-compliance, it
is not a general purpose file system.

QuantumFS is also not highly available on its own. QuantumFS is only as reliable
as the WSDB and Datastore.
