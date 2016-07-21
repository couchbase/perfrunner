#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi


env/bin/perfrunner -c ${cluster} -t ${test_config} ${override} --local
