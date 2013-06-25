#!/bin/bash -ex
virtualenv /tmp/prenv
/tmp/prenv/bin/pip install -r --upgrade requirements.txt
