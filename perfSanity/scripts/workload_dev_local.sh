#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi


$ENV_FOLDER/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} ${override} --nodebug --local
