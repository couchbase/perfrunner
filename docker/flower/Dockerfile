FROM ubuntu:16.04

MAINTAINER Pavel Paulau <pave@couchbase.com>

EXPOSE 5555

RUN apt-get update && apt-get install -y python-pip
RUN pip install flower

COPY run.sh /usr/local/bin/run.sh

CMD ["/usr/local/bin/run.sh"]
