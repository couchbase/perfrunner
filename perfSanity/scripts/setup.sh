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
        env/bin/install -c ${cluster} -v ${version}
    else
        env/bin/install -c ${cluster} --url ${url}
    fi
 else
    env/bin/install -c ${cluster} -v ${version} -t ${toy}
fi
env/bin/cluster -c ${cluster} -t ${test_config} ${override}
