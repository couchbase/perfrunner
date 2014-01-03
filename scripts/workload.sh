#!/bin/bash -ex
/tmp/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} stats.post_to_sf 1
