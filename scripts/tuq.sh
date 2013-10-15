#!/bin/bash -ex

/tmp/prenv${nickname}/bin/python -m perfrunner.utils.tuq -c ${cluster} -t ${test_config}
