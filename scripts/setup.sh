#!/bin/bash -ex
cat ${cluster}
cat ${test_config}

if [ -z "${toy}" ]; then
    /tmp/env/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version}
else
    /tmp/env/bin/python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
fi
/tmp/env/bin/python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}
