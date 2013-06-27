#!/bin/bash -ex
/tmp/prenv/bin/python -m perfrunner.runner -c ${cluster} -t ${test_config}
