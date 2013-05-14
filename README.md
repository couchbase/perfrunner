perfrunner
==========

Performance test framework for NoSQL databases

Requirements
------------

* Python 2.6+ (including headers)
* virtualenv
* libxml2-devel or equivalent
* libxslt-devel or equivalent

Cluster installation and setup
------------------------------

    python -m perfrunner.utils.install -c ${cluster} -v ${version}
    python -m perfrunner.utils.cluster -c ${cluster} -t ${test_config}
