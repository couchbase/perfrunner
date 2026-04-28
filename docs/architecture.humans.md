# Architecture — Human Reference

## System Overview

perfrunner is Couchbase's internal performance testing framework. It automates the full lifecycle
of a performance benchmark: provisioning clusters, configuring services, generating workloads,
collecting metrics, and reporting results to the showfast dashboard. The framework supports
on-premise bare-metal clusters, Kubernetes (CAO) deployments, and Capella (cloud DBaaS).

The system is designed around **declarative test configurations** (INI-style `.test` files) that
describe what to benchmark, and **cluster specifications** (`.spec` files) that describe the
infrastructure. A single CLI invocation combines these to run an end-to-end benchmark.

## Major Components and Responsibilities

### 1. Configuration System (`perfrunner/settings.py`)

The configuration layer is the backbone of perfrunner. Two main classes:

- **`ClusterSpec`** — parses `.spec` files to describe cluster topology: nodes, roles (KV, N1QL,
  FTS, Index, Analytics, Eventing), memory quotas, storage, and infrastructure type
  (on-prem, Kubernetes, Capella).

- **`TestConfig`** — parses `.test` files to describe everything about a benchmark: load phase
  settings, access phase settings, target buckets/scopes/collections, workload parameters,
  metric reporting categories, and which test class to run.

This file is very large (~4500 lines) because it encodes the full configuration surface of
Couchbase Server's many services and tuning knobs.

### 2. Test Orchestration (`perfrunner/tests/`)

Test classes inherit from `PerfTest` (defined in `__init__.py`) and implement a `run()` method.
Each test class represents a category of benchmark — KV throughput, N1QL latency, XDCR replication
rate, FTS indexing speed, etc.

The base `PerfTest` class acts as a **context manager** and wires together all the helpers:

- `ClusterManager` — configures the cluster before the test
- `WorkerManager` — starts/manages Celery workers for workload generation
- `Monitor` — waits for cluster operations to complete (rebalance, indexing)
- `MetricHelper` — computes final metrics from collected data
- `ShowFastReporter` — publishes results to the dashboard

A typical test lifecycle:
```
1. Parse configs → 2. Setup cluster → 3. Load data → 4. Run access workload
→ 5. Collect metrics → 6. Report results → 7. Tear down
```

### 3. Workload Generation (`spring/`, `perfrunner/workloads/`)

**spring** is the primary workload generator. It uses the Couchbase SDK to perform KV operations,
N1QL queries, and other data operations at configurable rates. Key components:

- `wgen3.py` (WorkloadGen) — the main workload generator for SDK 3+
- `docgen.py` — generates documents of various sizes and structures
- `cbgen3.py` / `cbgen4.py` — Couchbase client wrappers for SDK 3 and 4
- `fastdocgen.c` — C extension for high-performance document generation

**Specialized workloads** in `perfrunner/workloads/` handle YCSB, DCP drain, Sync Gateway,
TPC-DS, VectorDB benchmarks, and other non-spring workloads.

### 4. Distributed Workers (`perfrunner/helpers/worker.py`)

Workload generation is distributed using **Celery**:

- **Local mode**: SQLite-backed broker and result backend — used for single-machine runs
- **Remote mode**: RabbitMQ broker — workers run on dedicated machines that generate load
  against the cluster

`WorkloadPhase` encapsulates a phase of work (load or access) and distributes Celery task
signatures across available workers, with each task targeting a specific bucket and instance.

### 5. Metrics Collection (`cbagent/`, `perfrunner/metrics/`)

Two systems coexist:

- **cbagent (legacy)**: Collector classes poll Couchbase REST APIs (ns_server stats, cbstats,
  latency percentiles, FTS stats, etc.) and push time-series data to the cbmonitor service.

- **Prometheus (newer)**: Queries the Prometheus endpoint exposed by Couchbase Server, managed
  by `PrometheusAgent`. Controlled by the `use_prometheus` flag in test config.

`MetricHelper` and `PrometheusMetricsHelper` compute summary metrics (throughput, p99 latency,
index build time, etc.) from the collected data.

### 6. Cluster Management (`perfrunner/helpers/cluster.py`)

A factory pattern produces the right cluster manager:

- **`DefaultClusterManager`** — on-premise bare-metal or VM clusters (most common)
- **`KubernetesClusterManager`** — Couchbase Autonomous Operator (CAO) deployments
- **`CapellaClusterManager`** — Couchbase Capella (cloud DBaaS)

Cluster managers handle: bucket creation, index creation, service configuration, rebalancing,
failover, and other pre-test setup.

### 7. Infrastructure and Provisioning

- **`perfrunner/helpers/rest.py`** (~120KB) — comprehensive REST client for all Couchbase
  Server management APIs
- **`perfrunner/helpers/remote.py`** — SSH-based remote operations via fabric3
- **`perfrunner/utils/install.py`** — installs Couchbase Server on cluster nodes
- **`terraform/`** — Terraform configs for cloud infrastructure provisioning
- **`playbooks/`** — Ansible playbooks for server provisioning

### 8. Reporting (`perfrunner/helpers/reporter.py`)

`ShowFastReporter` posts benchmark results to the **showfast** dashboard
(`showfast.sc.couchbase.com`). Results include:

- Cluster metadata (topology, build version)
- Metric values (throughput, latency percentiles, times)
- Categories and subcategories for dashboard organization

## End-to-End Test Flow

A full benchmark is not a single command — it is a small pipeline of console scripts that
all live in `env/bin/` (defined as `[project.scripts]` in `pyproject.toml`). They each read
the same `.spec` and `.test` files but own a different stage of the lifecycle.

### CLI pipeline

```
   ┌────────────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────────┐   ┌────────────┐   ┌──────────────────────┐
   │ env/bin/       │ → │ env/bin/     │ → │ env/bin/     │ → │ env/bin/       │ → │ env/bin/   │ → │ env/bin/             │
   │ terraform      │   │ install      │   │ cluster      │   │ perfrunner     │   │ debug      │   │ terraform_destroy    │
   ├────────────────┤   ├──────────────┤   ├──────────────┤   ├────────────────┤   ├────────────┤   ├──────────────────────┤
   │ Provision VMs/ │   │ Download &   │   │ Configure    │   │ Run the actual │   │ Collect    │   │ Tear down all cloud  │
   │ Capella infra; │   │ install the  │   │ services,    │   │ benchmark, push│   │ logs, scan │   │ resources created in │
   │ write .spec    │   │ Couchbase    │   │ buckets,     │   │ metrics to     │   │ for crashes│   │ step 1               │
   │ (cloud only)   │   │ Server build │   │ rebalance,   │   │ cbmonitor &    │   │ & panics,  │   │ (cloud only)         │
   │                │   │ on every     │   │ TLS, RBAC,   │   │ results to     │   │ ship errors│   │                      │
   │                │   │ node         │   │ collections  │   │ showfast       │   │ to Loki    │   │                      │
   └────────────────┘   └──────────────┘   └──────────────┘   └────────────────┘   └────────────┘   └──────────────────────┘
        (optional)          mandatory          mandatory           mandatory          mandatory             (optional)
```

For static on-prem clusters the `terraform` and `terraform_destroy` steps are skipped — the
`.spec` already exists in `clusters/` and the machines are already up. For Capella tests,
`install` is also typically skipped because Capella owns the server build.

Each step is a thin Python entry point:

| Script | Module | Responsibility |
|---|---|---|
| `terraform` | `perfrunner/utils/terraform.py` | Wraps `CloudVMDeployer` + Capella public API. Provisions nodes and writes the resolved `.spec`. |
| `install` | `perfrunner/utils/install.py` | `CouchbaseInstaller` — finds the right build under `latestbuilds.service.couchbase.com`, copies/installs it on every node. |
| `cluster` | `perfrunner/utils/cluster.py` | Picks `DefaultClusterManager` / `KubernetesClusterManager` / `CapellaClusterManager` and runs all the pre-test setup (memory quotas, services, rebalance, buckets, indexes, TLS, encryption-at-rest, etc.). |
| `perfrunner` | `perfrunner/__main__.py` | Dynamically imports the test class named in the `.test` file and runs `PerfTest.run()`. |
| `debug` | `perfrunner/utils/debug.py` | Triggers `cbcollect_info` (or Capella log download to S3), unpacks zips, looks for Go panics / minidumps / storage corruption, pushes error lines to Loki, and on crash auto-installs the matching `*-debuginfo` package and generates backtraces. Fails the run if crashes are detected. |
| `terraform_destroy` | `perfrunner/utils/terraform.py:destroy` | Releases cloud resources. |

Within step 4, the perfrunner CLI itself does:

```
                    ┌──────────────┐
                    │  .test file  │  ← Test configuration (workload, settings, test class)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  .spec file  │  ← Cluster topology (nodes, roles, infra type)
                    └──────┬───────┘
                           │
              ┌────────────▼────────────┐
              │  perfrunner CLI         │  Parses configs, imports test class
              │  (__main__.py)          │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │  PerfTest.__init__      │  Wires ClusterManager, WorkerManager,
              │                         │  Monitor, MetricHelper, Reporter
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │  test.run()             │  Subclass-specific benchmark logic
              │                         │
              │  1. Cluster setup       │  ← ClusterManager configures buckets, indexes, etc.
              │  2. Load phase          │  ← Workers generate initial data
              │  3. Access phase        │  ← Workers run benchmark workload
              │  4. Metric collection   │  ← cbagent or Prometheus gathers stats
              │  5. Compute metrics     │  ← MetricHelper calculates final values
              │  6. Report to showfast  │  ← Reporter posts to dashboard
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │  PerfTest.__exit__      │  Debug checks, worker cleanup, profiling
              └─────────────────────────┘
```

## Deployment and Execution Model

perfrunner runs from a **driver machine** (often called the "perfrunner host") that:

1. Has SSH access to all cluster nodes (for provisioning and management)
2. Runs Celery workers locally or coordinates remote workers
3. Communicates with cbmonitor and showfast services

A typical Jenkins job invokes the pipeline shown above:

```
env/bin/terraform          -c $cluster -t $test          # cloud only
env/bin/install            -c $cluster -v $version
env/bin/cluster            -c $cluster -t $test
env/bin/perfrunner         -c $cluster -t $test
env/bin/debug              -c $cluster                   # always; fails build on crashes
env/bin/terraform_destroy  -c $cluster                   # cloud only
```

The four "core" steps (`install`, `cluster`, `perfrunner`, `debug`) are deliberately
separated so Jenkins can:

- re-run only the failing stage during debugging,
- archive the configured-but-not-tested cluster for inspection (`debug` independent of
  `perfrunner`),
- swap `terraform` for an on-prem-only flow without touching the rest.

Jenkins (external to this repo) is the CI system that triggers benchmark runs.

## Design Constraints and Tradeoffs

1. **INI-based configuration**: `.test` files use Python's ConfigParser. This limits
   expressiveness but keeps configs simple and diff-friendly. The tradeoff is that
   `settings.py` is enormous to handle all the parsing logic.

2. **Dynamic test class loading**: `exec(f"from {test_module} import {test_class}")`
   in `__main__.py`. This provides flexibility but means you can't statically analyze
   which test class a config uses without parsing the INI file.

3. **Celery for distribution**: Provides robust distributed task execution but adds
   complexity (broker management, serialization, worker lifecycle).

4. **Dual metric systems**: cbagent and Prometheus coexist for backward compatibility.
   New tests should prefer Prometheus when the cluster supports it.

5. **SDK version branching**: Separate code paths for Couchbase SDK 2, 3, and 4 in
   spring. This enables testing across SDK versions but means changes may need to be
   applied in multiple files.

## Risks and Future Cleanup Opportunities

- **settings.py at 4500+ lines** is a maintenance burden. Could benefit from splitting
  into per-service config modules.
- **rest.py at 120KB** — similar concern; service-specific REST methods could be modularized.
- **Legacy cbagent** will likely be deprecated in favor of Prometheus metrics.
- **Hardcoded internal IPs/hostnames** (RabbitMQ, cbmonitor, showfast) make the system
  tightly coupled to Couchbase's internal infrastructure.
- **No documented API contracts** for cbmonitor PerfStore or showfast — knowledge is
  embedded in code.
- **Test configs reference test classes by string** — a config typo only surfaces at runtime.
