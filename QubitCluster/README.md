# QuantumFS Docker Container
This directory contains an image designed to be able to quickly get a QuantumFS-enabled environment up and running. It will hopefully aid developers who would like to test things that depend on QFS outside of a production environment.

## Building The Image
To build an image with the default QFS version run `docker build -t qfs .`. This will create an image tagged as `qfs:latest`.

If you would like to use a different version then edit the `QFSVERSION` variable in the Dockerfile. The version you are trying to run must be available in http://dist/storage/QuantumFS/.

## Running A Container
To run a container use the following steps:
 - Grab a config file from the [Scylla Keyspace Reservations sheet](https://docs.google.com/spreadsheets/d/1ZAd-_rF0eqSqsllDF9rT7YWIDCbVwU4u_AgRJbeRvUM/edit#gid=0) and save it as `quantumfsd.conf`
 - Create and run an instance of the QFS container with `docker run --rm -it --privileged --shm-size=4g -v $PWD/quantumfsd.conf:/etc/quantumfsd.conf qfs bash`

This will start a shell in a container with a QuantumFS filesystem mounted on `/qfs`. The container will be removed once you exit the shell.

