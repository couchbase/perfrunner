Requirements
------------

* Python 2.6+ (including headers)
* virtualenv
* libcouchbase-devel
* dtach (for remote workers only)

Python dependencies are listed in requirements.txt.

Cluster installation and setup
------------------------------

    python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
    python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner.utils.install -c clusters/vesta.spec -v 2.0.0-1976

    python -m perfrunner.utils.install -c clusters/vesta.spec -v 2.1.1-PRF03 -t couchstore

    python -m perfrunner.utils.cluster -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Running tests
-------------

    python -m perfrunner.runner -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner.runner -c clusters/vesta.spec -t tests/comp_bucket_20M.test
