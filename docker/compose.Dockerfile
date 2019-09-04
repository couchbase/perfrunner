FROM ubuntu:16.04

RUN apt update && apt upgrade -y
RUN apt install -y build-essential git libcurl4-gnutls-dev libffi-dev libsnappy-dev libssl-dev lsb-core python3-dev python-pip python-virtualenv wget openssh-server sysstat

RUN wget http://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-4-amd64.deb
RUN dpkg -i couchbase-release-1.0-4-amd64.deb
RUN apt update && apt install -y libcouchbase-dev libcouchbase2-bin

RUN mkdir /var/run/sshd
RUN echo 'root:docker' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

WORKDIR /opt/perfrunner

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
