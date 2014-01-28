#!/bin/bash -ex
virtualenv -p python2.7 /tmp/env
/tmp/env/bin/pip install --download-cache /tmp/pip --upgrade -r requirements.txt
