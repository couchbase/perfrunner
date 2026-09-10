# Extended tier — feature tests, run at review time

The default home for new tests: feature tests, regression tests for a specific bug, and
characterisation tests protecting a refactor. Unlike `unittests/core/`, tests here may shell
out, open connections and touch the filesystem.

They run via `make test-extended` (inside `make review`) once per review round, not on every
patchset. A failure withholds Code-Review +1 — "not ready for a human yet" — and leaves the
Verified label alone.

Two rules:

- **Mirror the source tree** — a test for `perfrunner/remote/executor.py` goes in
  `perfrunner/remote/test_executor.py`. Repeating a basename is fine (`pyproject.toml` sets
  `--import-mode=importlib`); do not switch that back to `prepend`, which derives module names
  from the basename alone. The core tier is flat by contrast: it is capped by its admission
  criteria, so mirroring would buy it nothing.
- **Tests here can expire.** A test can be of high value during a change (or a chain of), low
  value after. The repo has no concept of an expiry. A broken test that nobody fixes may be
  deleted without discussion. If you are adding characterisation tests to protect a migration,
  say when they should be deleted, normally in the change that completes the migration.

See **Unit Test Tiers** in `AGENTS.md` for the criteria that promote a test to `unittests/core/`.
