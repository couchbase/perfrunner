FROM ubuntu:18.04

MAINTAINER Korrigan Clark <korrigan.clark@couchbase.com>

ARG DEBIAN_FRONTEND="noninteractive"

# set the variables as per $(pyenv init -)
ENV LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    PATH="/opt/pyenv/shims:/opt/pyenv/bin:$PATH" \
    PYENV_ROOT="/opt/pyenv" \
    PYENV_SHELL="bash"

RUN apt update && apt upgrade -y
RUN apt update
RUN apt install -y bash apt-utils make git libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev \
liblzma-dev python-openssl software-properties-common libcurl4-gnutls-dev \
libffi-dev libsnappy-dev libssl-dev lsb-core maven openjdk-8-jdk python3 \
python-pip python-virtualenv && rm -rf /var/lib/apt/lists/*

COPY pyenv-version.txt python-versions.txt /

RUN git clone -b `cat /pyenv-version.txt` --single-branch --depth 1 https://github.com/pyenv/pyenv.git $PYENV_ROOT \
    && for version in `cat /python-versions.txt`; do pyenv install $version; done \
    && pyenv global `cat /python-versions.txt` \
    && find $PYENV_ROOT/versions -type d '(' -name '__pycache__' -o -name 'test' -o -name 'tests' ')' -exec rm -rf '{}' + \
    && find $PYENV_ROOT/versions -type f '(' -name '*.pyo' -o -name '*.exe' ')' -exec rm -f '{}' + \
 && rm -rf /tmp/*

WORKDIR /opt
RUN git clone https://github.com/couchbase/perfrunner.git
WORKDIR /opt/perfrunner
RUN git fetch "http://review.couchbase.org/perfrunner" refs/changes/78/144178/9 && git checkout FETCH_HEAD
RUN make docker-cloud-worker


