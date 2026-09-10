# Build and Test Matrix — perfrunner

## Prerequisites

- Python 3.9.7 via pyenv
- Go (for Go utilities and misspell/gofmt checks)
- System libs for C extension: `libsnappy-dev`, `libssl-dev`, `libffi-dev`

## Setup

```bash
make                    # Creates env/, installs deps, editable install
```

## Validation Commands

| Check | Command | Scope | Speed |
|---|---|---|---|
| Lint | `make pep8` | `cbagent perfdaily perfrunner scripts spring unittests` | Fast (~seconds) |
| Unit tests — core | `make test` | `unittests/core/` (coverage for cbagent, perfrunner, spring) | Fast (~seconds) |
| Unit tests — extended | `make test-extended` | `unittests/extended/` | Seconds |
| **Unit tests — all tiers** | `make test-all` | core + extended + `unittests/local/` — **use this while developing** | Seconds |
| Misspell | `make misspell` | Go + Python source files | Fast |
| Go format | `make gofmt` | `go/` directory | Fast |
| **Per-patchset check** | `make check` | lint + misspell + gofmt + core tests | Fast |
| **Review-time check** | `make review` | `make check` + extended tests | Fast |

See **Unit Test Tiers** in `AGENTS.md` for which tier a new test belongs in.

## Go Build Targets

| Target | Command | Notes |
|---|---|---|
| cachestat | `make cachestat` | Standalone build |
| dcptest | `make dcptest` | Requires `vendor-sync` |
| cbindexperf | `make cbindexperf` | Requires `buildquery` + `vendor-sync` |
| kvgen | `make kvgen` | Requires `vendor-sync` |
| rachell | `make rachell` | Standalone build |
| loader | `make loader` | Standalone build |

## Docker

```bash
make docker             # Build perfrunner Docker image
make docker-compose     # Start local compose environment
```

## Recommended Pre-Commit Workflow

```bash
make pep8               # Lint first (fastest feedback)
make test               # Then unit tests
# If touching Go code:
make gofmt              # Go format check
```

## Notes

- `make check` is the comprehensive gate but requires Go toolchain for misspell/gofmt.
- For Python-only changes, `make pep8 && make test` is sufficient.
- There are no integration tests runnable without a live Couchbase cluster.
- Coverage is reported for `cbagent`, `perfrunner`, and `spring` packages.
