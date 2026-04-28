# perfrunner — AGENTS.md

Couchbase performance testing framework. Drives end-to-end perf benchmarks against
Couchbase Server clusters: load generation (spring), metric collection (cbagent),
test orchestration, and result reporting. Owned by the Couchbase Performance Team.

## Core Commands

| Action | Command |
|---|---|
| Create/update venv + install deps | `make` |
| Lint (ruff) | `make pep8` |
| Unit tests (nose + coverage) | `make test` |
| Full local checks (lint + misspell + gofmt + tests) | `make check` |
| Clean artifacts/logs/dbs | `make clean` |
| Show CLI help | `env/bin/perfrunner --help` |

- The venv lives at `./env/`; console scripts install into `env/bin/`.
- `make` uses Python `3.9.7` via `pyenv`.

## Repository Layout

| Path | Purpose |
|---|---|
| `perfrunner/` | Main Python package (entrypoint: `perfrunner.__main__:main`) |
| `perfrunner/settings.py` | All test/cluster config parsing (`ClusterSpec`, `TestConfig`) |
| `perfrunner/tests/` | Test classes by service area (kv, n1ql, fts, xdcr, analytics, etc.) |
| `perfrunner/helpers/` | Cluster REST API, monitoring, metrics, worker management |
| `perfrunner/utils/` | CLI entry-point scripts (install, cluster, deploy, destroy, terraform, etc.) |
| `perfrunner/workloads/` | Workload definitions (bigfun, YCSB, DCP, syncgateway, etc.) |
| `spring/` | Workload generator package + C extension (`fastdocgen.c`) |
| `cbagent/` | Metrics collection agent (collectors, stores, metadata) |
| `perfdaily/` | Daily perf reporting |
| `scripts/` | Supporting tooling |
| `tests/` | ~500+ `.test` config files (INI-style), organized by feature subdirs |
| `clusters/` | Cluster spec / inventory files (`*.spec`) |
| `playbooks/` | Ansible provisioning automation |
| `go/` | Go utilities: cachestat, dcptest, cbindexperf, kvgen, rachell, loader |
| `cloud/` | Cloud infrastructure specs |
| `terraform/` | Terraform configs for cloud provisioning |
| `docker/` | Dockerfile (Ubuntu 20.04, pyenv, multi-stage) |
| `templates/` | Jinja2 templates for config generation |
| `unittests.py` | Unit test file (run via `make test`) |

## Development Constraints

- Target Python version: **3.9** (`target-version = "py39"` in `.ruff.toml`).
- Line length: **100** characters.
- Linting: `ruff` with pydocstyle (D) + isort (I) + pycodestyle (W, E). See `.ruff.toml`.
- Type checking: `pyright` is configured (`pyrightconfig.json`) but `typeCheckingMode = "off"` — no strict gate.
- Unit tests use `nosetests` against `unittests.py` with `coverage`.
- Go code under `go/` must be `gofmt`-clean (see `make gofmt`).
- Keep CLI entry points in `perfrunner/utils/` thin; prefer logic in `perfrunner/`.
- Test config files in `tests/` are INI-style; parsed by `perfrunner.settings.TestConfig`.

## Validation and Evidence

Before claiming a change is complete, run and report output from:

1. `make pep8` — lint must pass with zero errors
2. `make test` — unit tests must pass

For Go changes, also run `make gofmt`. For full validation: `make check`.

Include the exact commands run and any relevant output snippet as evidence.

## Security and Sensitive Paths

**Do not** run commands that provision, reconfigure, or destroy clusters unless explicitly asked:
- `env/bin/install`, `env/bin/cluster`, `env/bin/destroy`
- `terraform*`, Ansible playbooks under `playbooks/`

Treat these as potentially sensitive (IPs, usernames, certs, keys):
- `clusters/`, `etc/`, `env/`, `cloud/`, `terraform/`
- `.capella_creds`, `.secrets.json`, `root.pem`, `certificates/`

Never log, display, or commit secrets.

## Supporting Context

- [`docs/architecture.agents.md`](docs/architecture.agents.md) — module boundaries, runtime flows, validation map
- [`docs/architecture.humans.md`](docs/architecture.humans.md) — narrative architecture overview
- [`docs/agent-context/repo-inventory.md`](docs/agent-context/repo-inventory.md) — languages, tools, key paths, unknowns
- [`docs/agent-context/build-test-matrix.md`](docs/agent-context/build-test-matrix.md) — exact validation commands by component
- [`docs/agent-context/domain-glossary.md`](docs/agent-context/domain-glossary.md) — Couchbase and perfrunner terminology
- [`docs/agent-context/troubleshooting.md`](docs/agent-context/troubleshooting.md) — common setup/test failures and fixes
