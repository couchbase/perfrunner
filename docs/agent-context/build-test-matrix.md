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
| Lint | `make pep8` | `cbagent perfdaily perfrunner scripts spring` | Fast (~seconds) |
| Unit tests | `make test` | `unittests.py` (coverage for cbagent, perfrunner, spring) | Fast (~seconds) |
| Misspell | `make misspell` | Go + Python source files | Fast |
| Go format | `make gofmt` | `go/` directory | Fast |
| **Full check** | `make check` | All of the above | Fast |

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
