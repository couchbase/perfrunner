"""Corpus validator: every class a test config names is defined in the module it names."""

import ast
import configparser
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.misc import pretty_dict

STAR_IMPORT = "*"

# Module-level statements that can still bind a name at module level
NESTING = (ast.If, ast.Try, ast.With, ast.For, ast.AsyncWith, ast.AsyncFor)


def bound_names(body: list) -> set:
    """Collect the names a list of statements binds in the enclosing namespace."""
    names = set()
    for node in body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, NESTING):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.stmt):
                    names |= bound_names([child])
                elif isinstance(child, ast.excepthandler):
                    names |= bound_names(child.body)
    return names


@lru_cache(maxsize=None)
def module_top_level_names(module_name: str) -> frozenset:
    """Collect the names a module binds at top level, without importing it.

    `perfrunner.helpers.worker` raises on import unless WORKER_TYPE is set, so the modules
    named by test configs cannot be imported in a core-tier test. Parsing keeps this pure.

    An empty set means the module file is missing. A set holding `STAR_IMPORT` means a star
    import brings in names parsing cannot see, so the module is left unchecked.
    """
    path = Path(module_name.replace(".", "/") + ".py")
    if not path.is_file():
        path = Path(module_name.replace(".", "/")) / "__init__.py"
    if not path.is_file():
        return frozenset()

    return frozenset(bound_names(ast.parse(path.read_text(), filename=str(path)).body))


class TestConfigTest(TestCase):
    def test_test_case_classes_resolve(self):
        """Check that every [test_case] `test` option names a real test class.

        `perfrunner.__main__` resolves the option as `from <module> import <class>`, so a
        rename that misses a `.test` file only surfaces once Jenkins has burned a run.
        """
        unresolved = defaultdict(list)

        for path in sorted(Path("tests").rglob("*.test")):
            parser = configparser.ConfigParser(interpolation=None)
            try:
                parser.read(path)
            except configparser.Error as e:
                unresolved[str(path)].append(f"unparseable: {e}")
                continue

            if not (dotted_path := parser.get("test_case", "test", fallback=None)):
                continue

            module_name, _, class_name = dotted_path.strip().rpartition(".")
            if not module_name:
                unresolved[str(path)].append(f"{dotted_path}: not a dotted path")
            elif not (names := module_top_level_names(module_name)):
                unresolved[str(path)].append(f"{dotted_path}: no module {module_name}")
            elif class_name not in names and STAR_IMPORT not in names:
                unresolved[str(path)].append(f"{dotted_path}: no {class_name} in {module_name}")

        self.assertEqual(
            {},
            dict(unresolved),
            f"Test configs naming a class that cannot be imported:\n{pretty_dict(unresolved)}",
        )
