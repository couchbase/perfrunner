#!/bin/bash -ex
/tmp/prenv${nickname}/bin/python -m perfrunner.runner -c ${cluster} -t ${test_config}
