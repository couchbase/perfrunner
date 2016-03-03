#!/bin/bash -ex

if [ -z "$ENV_FOLDER" ];
then
	ENV_FOLDER="/tmp"
fi

virtualenv -p python2.7 $ENV_FOLDER/env
PATH=/usr/lib/ccache:/usr/lib64/ccache/bin:$PATH $ENV_FOLDER/env/bin/pip install --upgrade -r requirements.txt
