#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

/tmp/prenv/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
/tmp/prenv/bin/python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}
