FROM ubuntu:16.04

MAINTAINER Pavel Paulau <pavel@couchbase.com>

RUN apt update && apt upgrade -y
RUN apt install -y build-essential git libcurl4-gnutls-dev libffi-dev libsnappy-dev libssl-dev lsb-core python3-dev python-pip python-virtualenv wget

RUN wget http://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-4-amd64.deb
RUN dpkg -i couchbase-release-1.0-4-amd64.deb
RUN apt update && apt install -y libcouchbase-dev

WORKDIR /opt
RUN git clone https://github.com/couchbase/perfrunner.git

WORKDIR /opt/perfrunner
RUN make
