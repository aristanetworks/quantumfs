# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.
FROM fedora:20

LABEL maintainer="clr@arista.com"

ENV USER root
ARG QFS_RPM_VERSION=latest

RUN yum install -y supervisor fuse wget && \
    wget -q http://dist/storage/QuantumFS/QuantumFS-$QFS_RPM_VERSION.x86_64.rpm && \
    yum --nogpgcheck -y localinstall QuantumFS-$QFS_RPM_VERSION.x86_64.rpm && \
    mkdir /qfs

COPY run_qfs.sh /run_qfs.sh
COPY quantumfs_supervisord.conf /etc/quantumfs_supervisord.conf

CMD ["supervisord", "-n", "-c", "/etc/quantumfs_supervisord.conf"]
