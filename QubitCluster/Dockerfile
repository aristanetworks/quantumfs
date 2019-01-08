# Copyright (c) 2017 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.
FROM fedora:20

LABEL maintainer="clr@arista.com"

ENV USER root
ARG QFS_RPM_VERSION

COPY Arista.repo /etc/yum.repos.d/
COPY install_qfs.sh /install_qfs.sh

RUN /install_qfs.sh $QFS_RPM_VERSION
RUN yum install -y telegraf
RUN mkdir /tmp/ninja_stats/

COPY run_qfs.sh /run_qfs.sh
COPY quantumfs_supervisord.conf /etc/quantumfs_supervisord.conf

CMD ["supervisord", "-n", "-c", "/etc/quantumfs_supervisord.conf"]
