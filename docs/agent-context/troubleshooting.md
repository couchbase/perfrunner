# Troubleshooting — perfrunner

## Setup Issues

### `make` fails with pyenv error

**Symptom:** `pyenv: command not found` or Python 3.9.7 not available.

**Fix:** Install pyenv and the required Python version:
```bash
pyenv install 3.9.7
pyenv local 3.9.7
```

### C extension build failure (`fastdocgen.c`)

**Symptom:** `error: command 'gcc' failed` or missing `snappy.h`.

**Fix:** Install system dependencies:
```bash
# Ubuntu/Debian
sudo apt install build-essential libsnappy-dev libssl-dev libffi-dev
```

### `couchbase` SDK install failure

**Symptom:** `pip install` fails on the `couchbase` package.

**Fix:** The Couchbase Python SDK requires `libcouchbase`. See [Couchbase SDK docs](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) for platform-specific install instructions.

## Lint Issues

### ruff reports errors on unchanged code

**Symptom:** `make pep8` fails on code you did not touch.

**Action:** Fix the lint errors anyway — they are pre-existing. The repo expects a clean lint pass. Common issues:
- Missing docstrings (D rules — but D100-D105, D107 are ignored)
- Import order (I rules — isort)
- Line length > 100 chars

### `cbagent/collectors/__init__.py` excluded from lint

This file is explicitly excluded in `.ruff.toml`. Do not add it to lint scope.

## Test Issues

### Unit tests fail on `.test` or `.spec` file parsing

**Symptom:** `SettingsTest` failures — usually means a `.test` or `.spec` file has invalid syntax.

**Action:** The unit tests parse **all** `.test` and `.spec` files. If you added or modified one, verify its INI syntax. Check for:
- Missing section headers (`[section]`)
- Duplicate keys in the same section
- Invalid value formats

### Tests require `snappy` module

**Symptom:** `ImportError: No module named 'snappy'`.

**Fix:** Ensure `python-snappy` is installed: `env/bin/pip install python-snappy`. Requires `libsnappy-dev` system package.

## Go Build Issues

### `make gofmt` fails

**Symptom:** `gofmt` reports formatting differences.

**Fix:** Run `gofmt -w go/` to auto-fix, then commit.

### `make misspell` — tool not found

**Symptom:** `misspell: command not found`.

**Fix:** The Makefile attempts `go get` for misspell. Ensure Go is installed and `GOPATH/bin` is in PATH.

## Runtime / Infra Notes

- Real test runs require live Couchbase clusters and worker machines — these cannot be run locally without infrastructure.
- Logs from test runs appear as `*.log` files in the repo root (gitignored).
- SQLite databases (`*.db`) are used for local result storage (gitignored).
- Jenkins is the likely CI system but pipeline definitions are external to this repo.

## Log Locations

| Log | Location |
|---|---|
| perfrunner runtime | `perfrunner/helpers/perfrunner.log`, `*.log` in repo root |
| celery workers | `async_worker*.log` in repo root |
| coverage report | `.coverage` in repo root |
