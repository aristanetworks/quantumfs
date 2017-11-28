# QuantumFS Docker Container
This directory contains an image designed to be able to quickly get a
QuantumFS-enabled environment up and running. It will hopefully aid developers
who would like to test things that depend on QFS outside of a production
environment.

## Building The Image
To build an image with the default QFS version run `docker build -t qfs .`.
This will create an image tagged as `qfs:latest`.

If you would like to use a different version then change the `QFS_RPM_VERSION`
build arg. E.g. `docker build -t qfs --build-arg QFS_RPM_VERSION=0.10.0-1 .`.
The version you are trying to run must be available in
http://dist/storage/QuantumFS/. Wget will print an error if the version
requested is invalid.

## Running A Container
To run a container use the following steps:
 - Grab a config file from the [Scylla Keyspace Reservations sheet](https://docs.google.com/spreadsheets/d/1ZAd-_rF0eqSqsllDF9rT7YWIDCbVwU4u_AgRJbeRvUM/edit#gid=0) and save it as `quantumfsd.conf`
 - Create and run an instance of the QFS container with `docker run --rm -it --privileged --shm-size=8g -v $PWD/quantumfsd.conf:/etc/quantumfsd.conf --name my_qfs_container qfs`
 - In a seperate terminal open a shell in the container with `docker exec -it my_qfs_container bash`

This will start a container with a QuantumFS filesystem mounted on `/qfs`.
The container will be removed once you send Ctrl-C to the `docker run` terminal.

---

Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
Arista Networks, Inc. Confidential and Proprietary.
