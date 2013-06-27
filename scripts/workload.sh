#!/bin/bash -ex
/tmp/prenv/bin/python -m perfrunner -c ${cluster} -t ${test_config}
