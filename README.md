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
* AMQP broker (RabbitMQ is recommended) for distributed workloads

Python dependencies are listed in requirements.txt. `make` does the magic.

SUT dependencies:
* numactl
* iostat

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

`--verbose` flag enables Fabric logging.

With `--local` flag localhost will be used as a workload generator.

Running functional tests
------------------------

    python -m perfrunner.tests.functional -c ${cluster} -t ${test_config}

For instance:

    python -m perfrunner.tests.functional -c clusters/atlas.spec -t tests/functional.test

Running unit tests
------------------

After `nose` installation:

    make test

Creating "Insight" experiments
------------------------------

cbmonitor provides handy APIs for experiments when we need to track metric while varying one or more input parameters. For instance, we want to analyze how GET latency depends on number of front-end memcached threads.

First of all, we create experiment config like [this one](https://github.com/couchbaselabs/perfrunner/blob/master/experiments/get_latency_threads.json):

    {
        "name": "95th percentile GET latency (ms), variable memcached threads",
        "defaults": {
            "memcached threads": 4
        }
    }

Query for `memcached threads` variable must be defined in experiment [helper](https://github.com/couchbaselabs/perfrunner/blob/master/perfrunner/helpers/experiments.py):

    'memcached threads': 'self.tc.cluster.num_cpus'

There must be corrensponding [test config](https://github.com/couchbaselabs/perfrunner/blob/master/tests/kv_hiload_600M_ro.test) which measures GET latency.
Most importantly test case should post experimental results:

    if hasattr(self, 'experiment'):
        self.experiment.post_results(latency_get)

Finally we can execute [scripts/workload_exp.sh](https://github.com/couchbaselabs/perfrunner/blob/master/scripts/workload_exp.sh) which has `-e` flag.

Now we can check cbmonitor [UI](http://cbmonitor.sc.couchbase.com/insight/) and analyze results.