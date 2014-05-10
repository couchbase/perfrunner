#!/bin/bash -ex

/tmp/env/bin/python -m perfrunner.utils.install_gw -c ${cluster} -t ${test_config} -v ${version_sgw} ${override}
