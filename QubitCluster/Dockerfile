FROM fedora:20

MAINTAINER clr@arista.com

ENV USER root
ENV QFSVERSION v0.8

RUN yum install -y \
      fuse \
      wget && \
    wget -q http://dist/storage/QuantumFS/QuantumFS-$QFSVERSION-1.x86_64.rpm && \
    yum --nogpgcheck -y localinstall QuantumFS-$QFSVERSION-1.x86_64.rpm && \
    mkdir /qfs

COPY run_qfs.sh /run_qfs.sh

ENTRYPOINT ["/run_qfs.sh"]
CMD ["bash"]
