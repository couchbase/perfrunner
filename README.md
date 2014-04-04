![](docs/perf_infra.png)

Related projects:
* [spring](https://github.com/couchbaselabs/spring)
* [cbagent](https://github.com/couchbaselabs/cbagent)
* [cbmonitor](https://github.com/couchbase/cbmonitor)
* [showfast](https://github.com/couchbaselabs/showfast)
* [seriesly](https://github.com/dustin/seriesly)
* [seriesly client](https://github.com/pavel-paulau/seriesly-python-client)
* [btrc](https://github.com/pavel-paulau/btrc)

Requirements
------------

* Python 2.7 (including headers)
* virtualenv
* libcouchbase-devel (or equivalent)
* dtach (for remote workers only)
* AMQP broker (RabbitMQ is recommended)

Python dependencies are listed in requirements.txt.

Cluster installation and setup
------------------------------

    python -m perfrunner.utils.install -c ${cluster} -v ${version} -t ${toy}
    python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner.utils.install -c clusters/vesta.spec -v 2.0.0-1976

    python -m perfrunner.utils.install -c clusters/vesta.spec -v 2.1.1-PRF03 -t couchstore

    python -m perfrunner.utils.cluster -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Running performance tests
-------------------------

    python -m perfrunner -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Overriding test config options (comma-separated section.option.value trios):

    python -m perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test \
        load.size.512,cluster.initial_nodes.3 4

Running functional tests
------------------------

    python -m perfrunner.tests.functional -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner.tests.functional -c clusters/atlas.spec -t tests/functional.test

Running unit tests
------------------

After `nose` installation:

    nosetests -v unittests.py
