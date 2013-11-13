#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

if [ -z "${toy}" ]; then
    /tmp/prenv${nickname}/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -b {vbuckets}
else
    /tmp/prenv${nickname}/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy} -b {vbuckets}
fi
/tmp/prenv${nickname}/bin/python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}
