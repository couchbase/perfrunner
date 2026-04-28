# Domain Glossary — perfrunner

## Couchbase Server Concepts

| Term | Meaning |
|---|---|
| KV | Key-Value service — core data storage and retrieval in Couchbase |
| N1QL | SQL-like query language for Couchbase (pronounced "nickel") |
| FTS | Full Text Search service |
| GSI | Global Secondary Index — index service for N1QL queries |
| XDCR | Cross Data Center Replication |
| DCP | Database Change Protocol — streaming protocol for data changes |
| Eventing | Server-side function execution service |
| Analytics | Couchbase Analytics Service (CBAS) |
| Magma | Storage engine (alternative to couchstore) for high-density workloads |
| Couchstore | Default storage engine |
| Plasma | Storage engine used by the Index (GSI) service |
| SGW / Sync Gateway | Mobile sync middleware between Couchbase Server and mobile clients |
| Bucket | Top-level data container in Couchbase (similar to a database) |
| vBucket | Virtual bucket — partition of a bucket for data distribution |
| DGM | Disk Greater than Memory — data set exceeds available RAM |
| Ephemeral | Bucket type that stores data only in memory |
| Rebalance | Process of redistributing data across cluster nodes |
| Failover | Removing a failed node from the cluster |
| Compaction | Reclaiming disk space by removing stale data |

## perfrunner Concepts

| Term | Meaning |
|---|---|
| `.test` file | INI-style test configuration file in `tests/` — defines workload, cluster settings, and metrics |
| `.spec` file | Cluster specification file in `clusters/` — defines cluster topology and node roles |
| spring | Workload generator package — generates documents, queries, and drives load |
| cbagent | Metrics collection agent — gathers stats from Couchbase nodes |
| showfast | Performance results dashboard — test configs reference categories/subcategories for it |
| perfrunner | The main test orchestrator — parses configs, sets up clusters, runs tests, collects metrics |
| `ClusterSpec` | Python class that parses `.spec` files |
| `TestConfig` | Python class that parses `.test` files |
| workers | Remote machines that generate workload against the cluster |
| celery | Distributed task queue used for remote worker coordination |
| fabric3 | SSH-based remote execution library used for cluster operations |

## Test Categories (from `.test` files)

| Category | Area |
|---|---|
| kv_* | Key-Value operations (throughput, latency, compaction) |
| n1ql_* / query_* | N1QL query benchmarks |
| fts_* | Full Text Search benchmarks |
| gsi_* / index_* | Secondary index benchmarks |
| xdcr_* | Cross-datacenter replication benchmarks |
| reb_* | Rebalance benchmarks (swap, failover, in/out) |
| tools_* | Backup, restore, import, export benchmarks |
| ycsb_* | Yahoo Cloud Serving Benchmark workloads |
| cloud/* | Capella (Couchbase cloud) specific tests |
| hidd/* | HIDD (High Item-Density Disk) tests |

## Acronyms

| Acronym | Expansion |
|---|---|
| YCSB | Yahoo Cloud Serving Benchmark |
| CBAS | Couchbase Analytics Service |
| MOI | Memory-Optimized Index |
| TLS / SSL | Transport Layer Security (encryption in transit) |
| OPS | Operations per second |
| HIDD | High Item-Density Disk |
| SOE | Sales Order Entry (workload type) |
| JTS | Java Test Suite (FTS test driver) |
