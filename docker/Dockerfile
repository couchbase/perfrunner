# This first phase builds the base OS and any necessary dependencies that will be used
# for the remainder of targets.
# This ensure we maintain the same core dependencies for local and k8s deployments.
FROM ubuntu:20.04 AS worker_image

LABEL org.opencontainers.image.authors="Performance Team (Couchbase)"

ARG DEBIAN_FRONTEND="noninteractive"
# Python versions (seprated by comma) to install in the created image
ARG PYTHON_VERSIONS="3.9.7,2.7.18"
# Pyenv version to install in the created image
ARG PYENV_INSTALL_VERSION="v2.4.0"

ENV LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    PATH="/opt/pyenv/shims:/opt/pyenv/bin:$PATH" \
    PYENV_ROOT="/opt/pyenv" \
    PYENV_SHELL="bash"

# Update the base image with any required dependencies
RUN apt update && apt upgrade -y
RUN apt update
RUN apt install -y bash apt-utils make git libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
    curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev \
    liblzma-dev python-openssl software-properties-common libcurl4-gnutls-dev \
    libffi-dev libsnappy-dev libssl-dev lsb-core maven openjdk-8-jdk python3 \
    python3-pip python3-virtualenv && rm -rf /var/lib/apt/lists/*

# Install Pyenv and any specified Python versions
RUN git clone -b $PYENV_INSTALL_VERSION --single-branch --depth 1 https://github.com/pyenv/pyenv.git $PYENV_ROOT \
    && for version in $(echo $PYTHON_VERSIONS | tr "," "\n"); do pyenv install $version; done \
    && pyenv global $(echo $PYTHON_VERSIONS | cut -d',' -f 1) \
    && find $PYENV_ROOT/versions -type d '(' -name '__pycache__' -o -name 'test' -o -name 'tests' ')' -exec rm -rf '{}' + \
    && find $PYENV_ROOT/versions -type f '(' -name '*.pyo' -o -name '*.exe' ')' -exec rm -f '{}' + \
    && rm -rf /tmp/*

# Fix No module named 'pip._vendor.six' issue until we update Python
RUN pip uninstall --yes virtualenv && pip3 uninstall --yes virtualenv && apt purge -y python3-virtualenv \
    && pip install virtualenv
# ---------------- END OF BASE WORKER IMAGE GENERATION --------------------


# This target installs perfrunner from master on top of the previous generated OS image
FROM worker_image AS perfrunner
WORKDIR /opt
RUN git clone https://github.com/couchbase/perfrunner.git
RUN git config --global user.email 'you@example.com' &&  git config --global user.name 'Perf Team'
WORKDIR /opt/perfrunner
RUN make docker-cloud-worker
# ---------------- END OF K8S WORKER IMAGE GENERATION --------------------


# Building perfrunner from local system for local docker composer usage
FROM worker_image AS compose
RUN apt update && apt upgrade -y
RUN apt update
RUN apt install -y wget openssh-server sysstat
RUN mkdir /var/run/sshd
RUN echo 'root:docker' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

WORKDIR /opt/perfrunner

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]