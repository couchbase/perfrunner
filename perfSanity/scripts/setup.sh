#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi

make
if [ -z "${toy}" ]; then
    if [ -z "${url}" ]; then
        env/bin/install -c ${cluster} --url=http://latestbuilds.hq.couchbase.com/couchbase-server/toy-sriram/32/couchbase-server-enterprise-4.7.0-4-centos6.x86_64.rpm -v 4.7.0-004
        env/bin/install -c ${cluster} -v ${version}
    else
        env/bin/install -c ${cluster} --url ${url} -v ${version}
    fi
 else
    env/bin/install -c ${cluster} -v ${version} -t ${toy}
fi
env/bin/cluster -c ${cluster} -t ${test_config} ${override}
