#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi


$ENV_FOLDER/env/bin/python -m perfrunner.utils.install_gw -c ${cluster} -t ${test_config} -v ${version_sgw} ${override}
