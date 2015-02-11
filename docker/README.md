
## Overview

Perfrunner is a tool to measure performance of Couchbase Server and Sync Gateway.

## Running

### Run docker image

```
$ sudo docker run -ti docker pull tleyden5iwx/perfrunner /bin/bash
```

### Set environment variables

```
$ export cluster="clusters/sync_gateway.spec"
$ export version="2.5.1-1087"
$ export version_sgw="0.0.1-27"
$ export override="gateload.pushers.50,gateload.pullers.80,gateway.num_nodes.2,gateway.profiling_freq.500,gateload.run_time.300"
```

### Kick off Perfrunner

```
$ cd /root/perfrunner
$ ./scripts/setup.sh
$ ./scripts/setup_sgw.sh
$ /tmp/env/bin/python -m perfrunner -c ${cluster} -t ${test_config} ${override}
```

## References

* [perfrunner](https://github.com/couchbaselabs/perfrunner)
* [gateload](https://github.com/couchbaselabs/gateload) - Sync Gateway load generator
* [jenkins perfrunner job](http://ci.sc.couchbase.com/view/sync%20gateway/job/cen006-p0-cbsgw-real-perf/)