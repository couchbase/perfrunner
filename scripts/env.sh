#!/bin/bash -ex
virtualenv -p python2.7 /tmp/env
/tmp/env/bin/pip install --upgrade -r requirements.txt
