# Code layout

QuantumFS is primarily written in golang and is divided into several packages.
The top level packet is intended to be imported into tools which use QuantumFS.
The other packages are either commands, public libraries or the internal
implementation.

Here is a brief description of each of the packages and their intended use:

quantumfs
    Top level package which contains definitions intended to be used by third
    party tools. This includes a golang implementation of the qfs API helpers
    and APIs to interpret data blocks.

QFSClient
    C++ implementation of the qfs API helpers.

backends
    Contains registration for various supported backends. A backend is an
    implementation of one of the core subsystems, such as the WSDB or datastore.

backends.processlocal
    In-memory backend implementations. Used mostly for tests.

backends.systemlocal
    File-based backend implementations. Used mostly for local testing or
    non-cluster configurations.

backends.grpc
    A grpc proxy backend for the WSDB. This is used as a simple,
    single-point-of-failure WSDB implementation which is accessible via the
    network.

cmd
    Contains the various binary packages.

cmd.emptykeys
    A development utility to compute the keys of the empty blocks.

cmd.qfs
    The primary user utility to access the non-filesystem capabilities of
    QuantumFS. This command implements a shell callable API bridge.

cmd.qloggerdb
    A utility to extract statistics from the binary tracing for offline
    analysis.

cmd.qparse
    A utility to parse the binary tracing for human consumption.

cmd.quantumfsd
    The QuantumFS daemon.

cmd.qupload
    A utility to quickly upload a filesystem tree directly into the QuantumFS
    datastore without going through FUSE.

cmd.qwalker
    A utility to walk the blocks of every workspace to ensure they are retained
    in the datastore.

cmd.wsdbhealthcheck
    A small utility which runs some fast queries against the WSDB to ensure
    basic healthiness. Useful for monitoring tools.

cmd.wsdbservice
    The daemon for the grpc WSDB server. This uses any of the WSDB backends to
    store the data.

daemon
    The primary implementation of the QuantumFS internals.

encoding
    Cap'n'Proto files for encoding and decoding the block formats.

features
    Not a golang package. This supplies build configuration when enabling
    various optional build features.

hash
    Contains golang adapters to the CityHash hash function used as the content
    hash in QuantumFS.

libqfs
    API helper code shared between quantumfs and QFSClient.

qfsclientc
    Adapter code which allows exercising C++ QFSClient code from golang. This is
    used to run QFSClient tests against a test QuantumFS instance.

qlog
    A fast binary tracing system.

qlogstats
    The core implementation of qloggerdb.

testutils
    Common test utilities, including the generic test framework definitions.

utils
    Various miscelaneous utilities.

walker
    A libary for walking QuantumFS block structures without running QuantumFS.
