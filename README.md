![](docs/perf_infra.png)

Related projects:
* [cbmonitor](https://github.com/couchbase/cbmonitor)
* [showfast](https://github.com/couchbaselabs/showfast)
* [seriesly](https://github.com/dustin/seriesly)
* [seriesly client](https://github.com/pavel-paulau/seriesly-python-client)
* [moveit](https://github.com/pavel-paulau/moveit)
* [dcp](https://github.com/couchbaselabs/python-dcp-client)

Installation
------------------------------

Before using perfrunner you should install the requirements listed in the requirements section. At a minimum you need Python 2.7, virtualenv, and libcouchbase.

First clone the perfrunner repo with the command below.

    git clone https://github.com/couchbaselabs/perfrunner.git

Once inside the perfrunner directory create a virtual environment for all of the perfrunner dependencies and install all of the dependencies so that you can run perfrunner.

    make

Cluster installation and setup
------------------------------

    ./env/bin/install -c ${cluster} -v ${version} -t ${toy}
    ./env/bin/cluster -c ${cluster} -t ${test_config}

For instance:

    ./env/bin/install -c clusters/vesta.spec -v 2.0.0-1976

    ./env/bin/install -c clusters/vesta.spec -v 2.1.1-PRF03 -t couchstore

    ./env/bin/cluster -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Running performance tests
-------------------------

    ./env/bin/perfrunner -c ${cluster} -t ${test_config}

For instance:

    ./env/bin/perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Overriding test config options (comma-separated section.option.value trios):

    ./env/bin/perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test \
        load.size.512,cluster.initial_nodes.3 4

`--verbose` flag enables Fabric logging.

`--nodebug` flag disables debug phase (e.g., execution of cbcollect_info).

With `--local` flag localhost will be used as a workload generator.

Running unit tests
------------------

Just run test target.

    make test
