# Repo Inventory — perfrunner

## Languages and Frameworks

| Language | Version | Role |
|---|---|---|
| Python | 3.9.7 | Primary (test framework, workload gen, metrics, CLI) |
| Go | (system) | Utilities: cachestat, dcptest, cbindexperf, kvgen, rachell, loader |
| C | n/a | Single extension: `spring/fastdocgen.c` |

## Package and Build Systems

| Tool | Config File | Notes |
|---|---|---|
| setuptools | `pyproject.toml`, `setup.py` | Editable install via `make` |
| pyenv | `.python-version` | Pins Python 3.9.7 |
| virtualenv | `env/` | Created by `make` |
| pip | `requirements.txt` | ~40 deps including couchbase SDK, celery, fabric3, ansible, boto3 |
| Make | `Makefile` | Primary build orchestrator |
| Go modules | `vendor/` | Vendored Go deps (govendor) |

## Test Frameworks and Entry Points

| Framework | Entry Point | Command |
|---|---|---|
| nosetests + coverage | `unittests.py` | `make test` |
| ruff | `.ruff.toml` | `make pep8` |
| misspell | Go-based | `make misspell` |
| gofmt | Go stdlib | `make gofmt` |

Unit tests cover: settings parsing, config validation, workload generators (docgen, key generators, query generators), spring iteration logic, and metric snapshot parsing.

## Key Directories

| Directory | Contents |
|---|---|
| `perfrunner/` | Core package: settings, helpers, tests, utils, workloads, metrics |
| `perfrunner/tests/` | Test classes (~28 modules) by Couchbase service: kv, n1ql, fts, xdcr, analytics, eventing, gsi, magma, syncgateway, tools, ycsb, etc. |
| `perfrunner/helpers/` | ~20 modules: REST API client, cluster ops, monitoring, metrics calculation, worker management, profiler, reporter |
| `perfrunner/utils/` | CLI entry-point scripts (~20): install, cluster, deploy, destroy, terraform, jenkins, stats, etc. |
| `perfrunner/workloads/` | Workload definitions: bigfun, analytics, YCSB, DCP, syncgateway, keyFragger, pathoGen, tcmalloc |
| `spring/` | Workload generator: docgen, querygen, wgen (worker generator), cbgen (Couchbase client gen), C extension |
| `cbagent/` | Metrics agent: collectors, metadata client, stores |
| `tests/` | ~500+ `.test` INI-style config files organized by feature subdirs |
| `clusters/` | Cluster spec files (`*.spec`) |
| `cloud/` | Cloud infra specs |
| `terraform/` | Terraform provisioning configs |

## Important Configs

| File | Purpose |
|---|---|
| `pyproject.toml` | Package metadata, console scripts, build config |
| `.ruff.toml` | Linter config (py39, line-length 100) |
| `pyrightconfig.json` | Type checking off (`typeCheckingMode: "off"`) |
| `ansible.cfg` | Ansible configuration |
| `compose.yaml` | Docker Compose for local dev |
| `Makefile` | Build, test, lint, Go build targets |
