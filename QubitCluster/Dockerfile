# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.
FROM fedora:20

LABEL maintainer="clr@arista.com"

ENV USER root
ARG QFS_RPM_VERSION=0.13.0-1

COPY install_qfs.sh /install_qfs.sh

RUN /install_qfs.sh $QFS_RPM_VERSION

COPY run_qfs.sh /run_qfs.sh
COPY quantumfs_supervisord.conf /etc/quantumfs_supervisord.conf

CMD ["supervisord", "-n", "-c", "/etc/quantumfs_supervisord.conf"]
