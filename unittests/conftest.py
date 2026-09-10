"""Shared pytest configuration. See Unit Test Tiers in AGENTS.md for the tier rules."""

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture(autouse=True, scope="session")
def run_from_repo_root():
    """Pin the cwd that the corpus validators' repo-relative globs depend on."""
    previous = Path.cwd()
    os.chdir(REPO_ROOT)
    yield
    os.chdir(previous)
