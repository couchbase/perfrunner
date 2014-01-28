#!/bin/bash -ex
virtualenv -p python2.7 /tmp/env
PATH=/usr/lib/ccache:/usr/lib64/ccache/bin:$PATH /tmp/env/bin/pip install --download-cache /tmp/pip --upgrade -r requirements.txt
