# Architecture — Agent Reference

Concise machine-oriented architecture for perfrunner. Paths are relative to repo root.

## Purpose and Scope

perfrunner orchestrates performance benchmarks against Couchbase Server clusters. It handles:
cluster configuration, workload generation, metric collection, and result reporting to the
showfast dashboard. It is **not** a unit/integration test framework — it is a perf harness.

## Layers and Key Paths

```
CLI Layer           perfrunner/__main__.py          Main entry: -c <spec> -t <test>
                    perfrunner/utils/*.py            ~20 utility CLIs (install, cluster, deploy, destroy, terraform, jenkins, etc.)
                    spring/__main__.py               Standalone workload generator CLI

Config Layer        perfrunner/settings.py           Parses .test + .spec files → TestConfig, ClusterSpec (INI via ConfigParser)
                    tests/**/*.test                  INI-style test configs (~500+)
                    clusters/*.spec                  Cluster topology specs

Test Classes        perfrunner/tests/__init__.py     Base class: PerfTest (context manager, orchestrates full run)
                    perfrunner/tests/*.py            ~28 modules by service: kv, n1ql, fts, xdcr, analytics, eventing,
                                                     gsi, magma, syncgateway, tools, ycsb, rebalance, etc.

Helpers             perfrunner/helpers/rest.py       REST API client for Couchbase Server (~120KB)
                    perfrunner/helpers/cluster.py    ClusterManager (factory → Default/Kubernetes/Capella variants)
                    perfrunner/helpers/monitor.py    Polls cluster state (rebalance, indexing, etc.)
                    perfrunner/helpers/metrics.py    Metric calculation from collected data
                    perfrunner/helpers/worker.py     WorkerManager + Celery task definitions
                    perfrunner/helpers/reporter.py   Posts results to showfast dashboard
                    perfrunner/helpers/local.py      Local shell command wrappers
                    perfrunner/helpers/remote.py     SSH remote execution (fabric3)
                    perfrunner/helpers/profiler.py   Linux perf profiling support

Workload Gen        spring/wgen.py, spring/wgen3.py  Workload generators (SDK 2 and 3+)
                    spring/docgen.py                  Document generators
                    spring/cbgen3.py, cbgen4.py       Couchbase client generators (SDK 3, 4)
                    spring/fastdocgen.c               C extension for fast doc generation
                    perfrunner/workloads/*.py          Specialized workloads (YCSB, DCP, syncgateway, bigfun, etc.)

Metrics             cbagent/                          Metrics collection agent
                    cbagent/collectors/*.py            ~20 collectors (system, ns_server, latency, FTS, eventing, etc.)
                    cbagent/stores.py                  PerfStore — pushes time-series to cbmonitor HTTP API
                    cbagent/metadata_client.py         Registers clusters/metrics with cbmonitor
                    perfrunner/metrics/cbagent.py      CbAgent-based metric agent
                    perfrunner/metrics/prometheus_agent.py  Prometheus-based metric agent (newer)

Infra               playbooks/*.yml                   Ansible provisioning
                    terraform/                         Cloud provisioning (AWS, GCP, Azure)
                    cloud/                             Cloud infrastructure specs
                    docker/Dockerfile                  Multi-stage: worker_image → perfrunner → compose
```

## Entry Points and Runtime Flow

### End-to-end pipeline (CLI sequence)

A full benchmark run is composed of several `env/bin/*` console scripts, all defined in
`pyproject.toml`'s `[project.scripts]` and backed by `perfrunner/utils/*.py`. The typical
order for a Jenkins job (or manual run) is:

```
env/bin/terraform   →   env/bin/install   →   env/bin/cluster   →   env/bin/perfrunner   →   env/bin/debug   →   env/bin/terraform_destroy
   (provision)         (install CB build)     (configure CB)        (run benchmark)         (collect logs)        (tear down)
```

| Step | Console script | Module | Purpose | Required for |
|---|---|---|---|---|
| 1 | `env/bin/terraform` | `perfrunner.utils.terraform:main` | Provision cloud infra (AWS/GCP/Azure VMs, EKS, Capella, App Services, Columnar) using Terraform + Capella API. Writes generated `.spec` and inventory files. | Cloud / Capella / k8s only — skipped for static on-prem clusters. |
| 2 | `env/bin/install` | `perfrunner.utils.install:main` | Resolve the Couchbase Server build from `latestbuilds`/releases, download the package, distribute it to all nodes (SSH/fabric), and install it. Optional `install_debug_sym`. | All deployments where perfrunner owns the build (i.e. not pre-baked Capella). |
| 3 | `env/bin/cluster` | `perfrunner.utils.cluster:main` | One-shot cluster configuration: memory/quotas, services, server groups, add-nodes, rebalance, buckets, RBAC, collections, indexes, TLS, encryption-at-rest, app telemetry. Dispatches to `DefaultClusterManager`, `KubernetesClusterManager`, or `CapellaClusterManager`. | All deployments. |
| 4 | `env/bin/perfrunner` | `perfrunner.__main__:main` | Run the actual benchmark: load → access → metric collection → report to showfast. | Always. |
| 5 | `env/bin/debug` | `perfrunner.utils.debug:main` | Post-run forensic step: trigger `cbcollect_info` (or Capella log collection → S3), download zips, scan for Go panics, minidumps and storage corruption, push error lines to Loki, and generate minidump backtraces (auto-installs the matching `*-debuginfo` package). Exits non-zero if crash files were found. | Always (failures here fail the Jenkins job). |
| 6 | `env/bin/terraform_destroy` | `perfrunner.utils.terraform:destroy` | Tear down cloud resources. | Cloud / Capella / k8s only. |

All steps share the same `-c <cluster_spec>` argument (and most also accept `-t <test_config>`),
so they read the same `ClusterSpec` / `TestConfig` view of the world. `terraform`'s deploy
phase actually *writes* the `.spec` (filling in node hostnames/IPs); subsequent steps consume it.

Other related console scripts: `deploy` (heavier multi-cloud orchestrator),
`destroy` (cluster-level teardown), `recovery`, `verify_logs`, `stats`, `templater`,
`x509_cert`, `sg_install` (Sync Gateway).

### Main test run: `env/bin/perfrunner -c <spec> -t <test>`

1. `perfrunner/__main__.py:main()` parses CLI args
2. `ClusterSpec.parse(<spec>)` → cluster topology
3. `TestConfig.parse(<test>)` → test configuration
4. Dynamically imports test class: `from {test_module} import {test_class}` (via `exec()`)
5. Runs test as context manager: `with TestClass(cluster_spec, test_config, verbose) as test: test.run()`

### Validation invariants between steps

- `install` must complete before `cluster` — `cluster` calls REST endpoints that only exist on
  a running `couchbase-server` process.
- `cluster` must complete before `perfrunner` — `PerfTest.__init__` assumes buckets/services are
  ready and only does *test-specific* setup (e.g. loading data, starting collectors).
- `debug` must run after `perfrunner` regardless of pass/fail — it is what surfaces server-side
  crashes and panics that the test itself would otherwise hide.
- `terraform` (deploy) and `terraform_destroy` are paired; `destroy` is safe to run even if
  `deploy` partially failed (idempotent-ish — uses Terraform state + Capella IDs in the spec).

### PerfTest lifecycle (`perfrunner/tests/__init__.py`):

```
__init__  → ClusterManager, RestHelper, Monitor, RemoteHelper, MetricHelper/PrometheusMetricsHelper,
             WorkerManager, ShowFastReporter
__enter__ → returns self
run()     → subclass-defined: typically load phase → access phase → metric collection → report
__exit__  → cleanup collector agent, debug checks (core dumps, rebalance), tear down workers
```

### Worker architecture:

- **Local mode**: Celery workers with SQLite broker (`perfrunner.db`) and result backend (`results.db`)
- **Remote mode**: Celery workers with AMQP broker (RabbitMQ at `172.23.96.202:5672`)
- Workers execute tasks defined in `perfrunner/helpers/worker.py` (spring_task, ycsb_task, etc.)
- `WorkloadPhase` distributes work across targets × instances → Celery group signatures

### Metric collection:

- **Legacy (cbagent)**: collectors poll Couchbase REST APIs → push to cbmonitor PerfStore (HTTP)
- **Prometheus**: PrometheusAgent queries Prometheus endpoint → stores snapshots
- `MetricHelper` / `PrometheusMetricsHelper` compute final metrics from collected data
- `ShowFastReporter` posts results to `showfast.sc.couchbase.com`

## External Systems and Environment Dependencies

| System | Purpose | Connection |
|---|---|---|
| Couchbase Server cluster | System under test | REST API (8091+), SDK (11210+) |
| cbmonitor (`cbmonitor.sc.couchbase.com`) | Time-series metric storage | HTTP :8080 |
| showfast (`showfast.sc.couchbase.com`) | Results dashboard | HTTP API |
| RabbitMQ (`172.23.96.202:5672`) | Remote worker message broker | AMQP |
| Worker machines | Workload generators | SSH (fabric3), Celery |
| AWS/GCP/Azure | Cloud provisioning | boto3, gcloud, Terraform |
| Jenkins | CI orchestration | python-jenkins |

## Validation Map

| What to validate | How | Test location |
|---|---|---|
| End-to-end pipeline wiring | manual run | `terraform` → `install` → `cluster` → `perfrunner` → `debug` (→ `terraform_destroy`) |
| Config parsing (.test/.spec) | `make test` | `unittests.py::SettingsTest` — parses ALL .test and .spec files |
| Document generation | `make test` | `unittests.py::DocGenTest`, `SpringTest` |
| Query generation | `make test` | `unittests.py` — N1QL query gen tests |
| Workload iteration | `make test` | `unittests.py::WorkloadTest` — key/value size validation |
| Code style | `make pep8` | ruff against cbagent, perfdaily, perfrunner, scripts, spring |
| Go formatting | `make gofmt` | `go/` directory |

## Unsafe and Sensitive Areas

- `perfrunner/utils/install.py`, `cluster.py`, `destroy.py`, `terraform.py` — provision/destroy infra
- `playbooks/` — Ansible playbooks that modify clusters
- `clusters/`, `cloud/`, `terraform/` — contain IPs, topology, potentially sensitive data
- `.capella_creds`, `.secrets.json`, `certificates/`, `root.pem` — credentials and certs
- `exec()` in `__main__.py` — dynamically loads test class from config; config is trusted input
- Hardcoded internal IPs: `172.23.96.202` (RabbitMQ), `cbmonitor.sc.couchbase.com`, `showfast.sc.couchbase.com`

## Common Agent Traps

1. **settings.py is ~4500 lines** — do not read it entirely; search for specific classes/settings.
2. **Test classes are NOT unit tests** — `perfrunner/tests/*.py` are perf test orchestrators, not pytest/unittest.
3. **`unittests.py` parses ALL .test and .spec files** — adding a malformed config file will break unit tests.
4. **Two metric systems coexist** — cbagent (legacy) and Prometheus (newer). Check `use_prometheus` flag.
5. **Three cluster manager types** — `DefaultClusterManager`, `KubernetesClusterManager`, `CapellaClusterManager` — factory in `ClusterManager.__new__`.
6. **SDK version branching** — spring has separate code paths for SDK 2, 3, and 4 (`cbgen.py`, `cbgen3.py`, `cbgen4.py`, `wgen.py`, `wgen3.py`).
7. **rest.py is ~120KB** — the largest helper; search don't read.

## Unknowns

- Jenkins pipeline definitions are external — not in this repo.
- Exact set of required environment variables for remote runs is undocumented.
- showfast dashboard schema/API is not documented in this repo.
- cbmonitor PerfStore API contract is inferred from `cbagent/stores.py` usage.
