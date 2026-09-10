# perfrunner — AGENTS.md

Couchbase performance testing framework. Drives end-to-end perf benchmarks against
Couchbase Server clusters: load generation (spring), metric collection (cbagent),
test orchestration, and result reporting. Owned by the Couchbase Performance Team.

## Core Commands

| Action | Command |
|---|---|
| Create/update venv + install deps | `make` |
| Lint (ruff) | `make pep8` |
| Unit tests — all tiers, use this while developing | `make test-all` |
| Unit tests — core tier only (what every patchset runs) | `make test` |
| Unit tests — extended tier only | `make test-extended` |
| Per-patchset CI checks (lint + misspell + gofmt + core tests) | `make check` |
| Review-time CI checks (`make check` + extended tests) | `make review` |
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
| `sdks/` | SDK-specific helper programs (dotnet/go/java config-push benchmarks) |
| `tests/` | ~500+ `.test` config files (INI-style), organized by feature subdirs |
| `collections/` | JSON bucket/scope/collection topology configs used by test configs |
| `clusters/` | Cluster spec / inventory files (`*.spec`) |
| `certificates/` | TLS auto-generated certs/keys used by cluster deployments (sensitive, see below) |
| `playbooks/` | Ansible provisioning automation |
| `go/` | Go utilities: cachestat, dcptest, cbindexperf, kvgen, rachell, loader |
| `cloud/` | Cloud infrastructure specs |
| `terraform/` | Terraform configs for cloud provisioning |
| `docker/` | Dockerfile (Ubuntu 20.04, pyenv, multi-stage) |
| `templates/` | Jinja2 templates for config generation |
| `unittests/` | Tiered unit tests — `core/` (gates every patchset), `extended/` (review-time), `local/` (gitignored scratch). See **Unit Test Tiers** |

## Development Constraints

- Target Python version: **3.9** (`target-version = "py39"` in `.ruff.toml`).
- Line length: **100** characters.
- Linting: `ruff` with pydocstyle (D) + isort (I) + pycodestyle (W, E). See `.ruff.toml`.
- Type checking: no static type checker is configured.
- Unit tests use `pytest` and are split into tiers by directory — see **Unit Test Tiers**
  below before adding any test.
- Go code under `go/` must be `gofmt`-clean (see `make gofmt`).
- Keep CLI entry points in `perfrunner/utils/` thin; prefer logic in `perfrunner/`.
- Test config files in `tests/` are INI-style; parsed by `perfrunner.settings.TestConfig`.

## Unit Test Tiers

perfrunner is an automation harness: the real evidence a change works is a green Jenkins perf
run, not a unit test. Unit tests catch the breakages that would waste those runs. **A test's
tier is its directory**, so the tier is visible in the Gerrit diff.

### Where to put a new test

**Default to `unittests/extended/`.** Feature tests, regression tests for a specific bug, and
characterisation tests protecting a refactor. They run before a human reviews the change, but
not on all 100+ patchsets it takes to get there.

**`unittests/core/` requires all three**, plus a justification in the commit message:

1. **Blast radius** — breaking it breaks many perf runs at once (settings parsing, docgen
   determinism, worker dispatch), *or* it is a corpus validator over `tests/`, `clusters/` or
   `tests/pipelines/`. A malformed `.test` file wastes an entire Jenkins run.
2. **Purity** — no subprocess, socket, SSH, `openssl` or `sleep`; no filesystem access beyond
   `tmp` and reading repo files. Enforced by `unittests/core/test_tier_guard.py`.
3. **Determinism** — same verdict on macOS and Linux, and across dependency versions.

**Layout differs by tier.** `core/` is flat: capped by the criteria above, small enough to
scan. `extended/` mirrors the source tree, since it is uncapped. Repeating a basename across
the two is fine; `pyproject.toml` sets `--import-mode=importlib`.

**`unittests/local/` is scratch** — throwaway tests, never committed, never run in CI.

### Tests have an expiry

Extended tests may be deleted without discussion if they break and are not fixed promptly.
**Characterisation tests protecting a migration should be deleted when the migration lands, in
the same change** — say so when adding them.

## Validation and Evidence

Before claiming a change is complete, run and report output from:

1. `make pep8` — lint must pass with zero errors
2. `make test-all` — core, extended and your own local tests must pass

`make test-all` is the right command while developing: it covers every tier including your
gitignored `unittests/local/` scratch. The narrower targets exist for CI —
`make check` (what runs on every patchset) and `make review` (what runs when the change is sent
for review). For Go changes, also run `make gofmt`.

Include the exact commands run and any relevant output snippet as evidence.

## Security and Sensitive Paths

**Do not** run commands that provision, reconfigure, or destroy clusters unless explicitly asked:
- `env/bin/install`, `env/bin/cluster`, `env/bin/destroy`
- `terraform*`, Ansible playbooks under `playbooks/`

Treat these as potentially sensitive (IPs, usernames, certs, keys):
- `clusters/`, `env/`, `cloud/`, `terraform/`
- `.capella_creds`, `.secrets.json`, `root.pem`, `certificates/`

Never log, display, or commit secrets.

## Supporting Context

- [`docs/architecture.agents.md`](docs/architecture.agents.md) — module boundaries, runtime flows, validation map
- [`docs/architecture.humans.md`](docs/architecture.humans.md) — narrative architecture overview
- [`docs/agent-context/repo-inventory.md`](docs/agent-context/repo-inventory.md) — languages, tools, key paths, unknowns
- [`docs/agent-context/build-test-matrix.md`](docs/agent-context/build-test-matrix.md) — exact validation commands by component
- [`docs/agent-context/domain-glossary.md`](docs/agent-context/domain-glossary.md) — Couchbase and perfrunner terminology
- [`docs/agent-context/troubleshooting.md`](docs/agent-context/troubleshooting.md) — common setup/test failures and fixes
