"""Guard that keeps the core tier fast, pure and deterministic.

Core runs on every patchset and owns Verified, so a test that reaches out to the machine it
runs on -- subprocess, socket, SSH, sleep -- makes every patchset hostage to whatever
environment CI has that day. Those belong in unittests/extended/ instead.
"""

import ast
from pathlib import Path
from unittest import TestCase

CORE_DIR = Path(__file__).parent

# Modules that pull the test out of the process and into the environment.
FORBIDDEN_IMPORTS = {
    "asyncio": "spawns event loops and is timing-sensitive",
    "celery": "needs a broker",
    "fabric": "opens SSH connections",
    "http": "makes network calls",
    "invoke": "shells out",
    "paramiko": "opens SSH connections",
    "requests": "makes network calls",
    "socket": "makes network calls",
    "subprocess": "shells out",
    "urllib": "makes network calls",
}

# Calls that make a test slow or non-deterministic regardless of what was imported.
FORBIDDEN_CALLS = {
    ("time", "sleep"): "makes the gate slow and flaky; restructure or move to extended/",
    ("os", "system"): "shells out",
    ("os", "fork"): "forks the test runner",
}


def _scan(tree):
    """Yield (lineno, what, why) for each escape from the process.

    Two passes: the first records what each name is bound to, so an aliased import cannot
    smuggle a banned call past the second. Importing a banned callable is itself a violation,
    which keeps `from time import sleep` from needing a per-function alias map.
    """
    modules, violations = {}, []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                modules[alias.asname or root] = root
                if why := FORBIDDEN_IMPORTS.get(root):
                    violations.append((node.lineno, f"import {alias.name}", why))
        elif isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            if why := FORBIDDEN_IMPORTS.get(root):
                violations.append((node.lineno, f"from {node.module} import ...", why))
                continue
            for alias in node.names:
                if why := FORBIDDEN_CALLS.get((root, alias.name)):
                    what = f"from {node.module} import {alias.name}"
                    violations.append((node.lineno, what, why))

    for node in ast.walk(tree):
        func = node.func if isinstance(node, ast.Call) else None
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            # Resolve through any alias, so `import time as clock` cannot hide clock.sleep().
            module = modules.get(func.value.id, func.value.id)
            if why := FORBIDDEN_CALLS.get((module, func.attr)):
                violations.append((node.lineno, f"{func.value.id}.{func.attr}()", why))

    return sorted(violations)


class CoreTierGuardTest(TestCase):
    def test_core_tier_stays_pure(self):
        """Core tests must not touch the network, the shell, or the clock."""
        violations = []

        for path in sorted(CORE_DIR.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            violations.extend(
                f"  {path.relative_to(CORE_DIR)}:{lineno}: {what} - {why}"
                for lineno, what, why in _scan(tree)
            )

        if violations:
            self.fail(
                "\nThese core tests reach outside the process, which makes the per-patchset "
                "gate depend on the CI environment:\n\n"
                + "\n".join(violations)
                + "\n\nMove them to unittests/extended/ (runs at review time via `make review`), "
                + "or rewrite them to test the logic without the I/O."
            )
