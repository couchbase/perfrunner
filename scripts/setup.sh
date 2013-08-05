#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

if [ -z "${toy}" ]; then
    /tmp/prenv/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version}
else
    /tmp/prenv/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
fi
/tmp/prenv/bin/python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}
