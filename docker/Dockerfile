FROM tleyden5iwx/centos-6-python-2.7

MAINTAINER Traun Leyden <tleyden@couchbase.com>

# Install libcouchbase
RUN wget -O/etc/yum.repos.d/couchbase.repo http://packages.couchbase.com/rpm/couchbase-centos62-x86_64.repo && \
    yum install -y \
    libcouchbase2-libevent \
    libcouchbase-devel

# Install perfrunner
RUN cd /root && \
    git clone https://github.com/couchbaselabs/perfrunner && \
    cd perfrunner && \
    sh scripts/env.sh
    