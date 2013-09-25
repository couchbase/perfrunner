#!/bin/bash -ex
/tmp/env/bin/python -m perfrunner.runner -c ${cluster} -t ${test_config}
