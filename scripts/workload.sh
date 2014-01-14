#!/bin/bash -ex
PYCBC_ASSERT_CONTINUE=1 /tmp/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} stats.post_to_sf.1
