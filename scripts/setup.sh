#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi


if [ -z "${toy}" ]; then
    if [ -z "${url}" ]; then
        $ENV_FOLDER/env/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version}
    else
        $ENV_FOLDER/env/bin/python -m perfrunner.utils.install -c ${cluster} --url=${url}
    fi
 else
    $ENV_FOLDER/env/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
fi
$ENV_FOLDER/env/bin/python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config} ${override}
