#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi

if [ -z "${override}" ]; then
    PYCBC_ASSERT_CONTINUE=1 $ENV_FOLDER/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} stats.post_to_sf.1
else
    PYCBC_ASSERT_CONTINUE=1 $ENV_FOLDER/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} ${override}
fi
