perfrunner
==========

Performance test framework for NoSQL databases

Requirements
------------

* Python 2.6+ (including headers)
* virtualenv
* libxml2-devel or equivalent
* libxslt-devel or equivalent
* libcouchbase

Python dependencies are listed in requirements.txt.

Cluster installation and setup
------------------------------

    python -m perfrunner.utils.install -c ${cluster} -v ${version}
    python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}

Running tests
-------------

    python -m perfrunner.runner -c ${cluster} -t ${test_config} ${test_case}

For instance:

    python -m perfrunner.runner -c clusters/vesta.spec -t tests/sample.test KVTest
