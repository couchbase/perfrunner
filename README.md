perfrunner
------------

[![codebeat badge](https://codebeat.co/badges/7870f2d2-4a41-477e-af30-d9a8cf097626)](https://codebeat.co/projects/github-com-couchbase-perfrunner)

Installation
------------

Before using perfrunner you should install the requirements listed in the requirements section. At a minimum you need

* Python 3.5
* virtualenv
* libcouchbase
* libcurl4-gnutls-dev
* libffi
* libsnappy
* libssl

Note, you should be able to install both client and server system dependencies using the Ansible playbooks (namely, clients.yml and servers.yml).

    ansible-playbook ${playbook} -i ${ini_file}
    
For instance:
    
    ansible-playbook  playbooks/servers.yml -i clusters/vesta.ini

First clone the perfrunner repo with the command below:

    git clone https://github.com/couchbase/perfrunner.git

As some components are written in Go, make sure that perfrunner is placed inside $GOPATH/src.
See also [How to Write Go Code](https://golang.org/doc/code.html).

Once inside the perfrunner directory create a virtual environment for all of the perfrunner dependencies and install all of the dependencies so that you can run perfrunner:

    make

Alternatively, you can make use of the supplied docker-compose file to run perfrunner in a Ubuntu Docker container, if you wish to avoid installing dependencies on your host machine. See [DOCKER-COMPOSE.md](docker/DOCKER-COMPOSE.md).

Cluster installation and setup
------------------------------

    env/bin/install -c ${cluster} -v ${version}
    env/bin/cluster -c ${cluster} -t ${test_config}

For instance:

    env/bin/install -c clusters/vesta.spec -v 4.5.0-2601

    env/bin/cluster -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Running performance tests
-------------------------

    env/bin/perfrunner -c ${cluster} -t ${test_config}

For instance:

    env/bin/perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test

Overriding the test settings (space-separated section.option.value trios):

    env/bin/perfrunner -c clusters/vesta.spec -t tests/comp_bucket_20M.test \
        "load.size.512" "cluster.initial_nodes.3 4"

`--verbose` flag enables Fabric logging.

With `--remote` flag remote workers will be used as workload generators.

Unit tests
----------

Just run the test target:

    make test

Related projects
----------------

* [cbmonitor](https://github.com/couchbase/cbmonitor)
* [showfast](https://github.com/couchbaselabs/showfast)
