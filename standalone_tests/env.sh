#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp/standalone_pip"
fi

virtualenv -p python2.7 $ENV_FOLDER/env
$ENV_FOLDER/env/bin/pip install --upgrade -r standalone_tests/requirements.txt
