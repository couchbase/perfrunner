"""Local command execution with Fabric 1 semantics, built on subprocess.

Drop-in replacement for the ``fabric.api`` local-side primitives (``local``, ``lcd``, ``shell_env``,
``hide``, ``quiet``, ``settings``, ``warn_only``) so that purely local helpers do not depend on an
SSH library. State is held in context variables instead of Fabric's process-wide ``env``,
which makes the context managers thread-safe.
"""

import os
import subprocess
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from typing import Optional

from logger import logger

_lcwd: ContextVar[str] = ContextVar("lcwd", default="")
_env_vars: ContextVar[dict] = ContextVar("env_vars", default={})
_warn_only: ContextVar[bool] = ContextVar("warn_only", default=False)
_hidden: ContextVar[frozenset] = ContextVar("hidden", default=frozenset())

# Global output switches, shared with perfrunner.remote.api (fabric.state.output compat).
# RemoteHelper sets these from its verbose flag; with stdout off, non-captured local()
# output is discarded like Fabric 1 did, otherwise nohup'd children (e.g. celery and
# the spring workers it forks) inherit the console and spam it (taskset & co).
# stderr is keyed separately (Fabric 1 parity): RemoteHelper never turns it off, so
# error text from local commands stays visible in non-verbose runs.
output = SimpleNamespace(running=True, stdout=True, stderr=True)


class AttributeString(str):
    """Fabric-style command result: the captured stdout with execution attributes."""

    command: str = ""
    real_command: str = ""
    return_code: int = 0
    stderr: str = ""

    @property
    def stdout(self) -> str:
        return str(self)

    @property
    def failed(self) -> bool:
        return self.return_code != 0

    @property
    def succeeded(self) -> bool:
        return not self.failed


def _is_hidden(group: str) -> bool:
    hidden = _hidden.get()
    if group in hidden or "everything" in hidden:
        return True
    if group == "running":
        return not output.running
    if group == "output":
        return not output.stdout
    if group == "stderr":
        # hide("output") covers both streams, like Fabric 1's 'output' alias
        return "output" in hidden or not output.stderr
    return False


@contextmanager
def lcd(path: str) -> Generator[None, None, None]:
    """Change the local working directory; relative paths nest like Fabric's ``lcd``."""
    current = _lcwd.get()
    if current and not os.path.isabs(path):
        path = os.path.join(current, path)
    token = _lcwd.set(path)
    try:
        yield
    finally:
        _lcwd.reset(token)


@contextmanager
def shell_env(**env_vars: str) -> Generator[None, None, None]:
    """Export the given environment variables for nested ``local()`` calls."""
    token = _env_vars.set({**_env_vars.get(), **env_vars})
    try:
        yield
    finally:
        _env_vars.reset(token)


@contextmanager
def hide(*groups: str) -> Generator[None, None, None]:
    """Hide the given output groups ("running", "output", "warnings", "everything")."""
    token = _hidden.set(_hidden.get() | frozenset(groups))
    try:
        yield
    finally:
        _hidden.reset(token)


@contextmanager
def warn_only() -> Generator[None, None, None]:
    """Turn command failures into warnings instead of aborting."""
    token = _warn_only.set(True)
    try:
        yield
    finally:
        _warn_only.reset(token)


@contextmanager
def quiet() -> Generator[None, None, None]:
    """Hide all output and turn failures into warnings."""
    with hide("everything"), warn_only():
        yield


@contextmanager
def settings(*context_managers, **env_settings) -> Generator[None, None, None]:
    """Apply nested context managers and/or ``warn_only=True``, like Fabric's ``settings``."""
    if unknown := set(env_settings) - {"warn_only"}:
        raise TypeError(f"Unsupported settings: {unknown}")
    with ExitStack() as stack:
        for manager in context_managers:
            stack.enter_context(manager)
        if env_settings.get("warn_only"):
            stack.enter_context(warn_only())
        yield


def local(command: str, capture: bool = False, shell: Optional[str] = None) -> AttributeString:
    """Run a command locally through the shell, mirroring Fabric 1 ``local()``.

    Failures abort with ``SystemExit`` unless running under ``warn_only()``/``quiet()``.
    With ``capture=True`` the returned string is the stripped stdout.
    """
    given_command = command
    if env_vars := _env_vars.get():
        exports = " ".join(f'{key}="{value}"' for key, value in env_vars.items())
        command = f"export {exports} && {command}"
    if lcwd := _lcwd.get():
        command = f"cd {lcwd} && {command}"

    if not _is_hidden("running"):
        logger.info(f"[localhost] local: {given_command}")

    if capture:
        out_stream, err_stream = subprocess.PIPE, subprocess.PIPE
    else:
        # None inherits the parent's stream; the two are keyed separately like Fabric 1,
        # so non-verbose runs discard stdout but keep error text visible
        out_stream = subprocess.DEVNULL if _is_hidden("output") else None
        err_stream = subprocess.DEVNULL if _is_hidden("stderr") else None

    process = subprocess.Popen(
        command, shell=True, executable=shell, stdout=out_stream, stderr=err_stream
    )
    stdout, stderr = process.communicate()

    result = AttributeString(stdout.decode(errors="replace").strip() if stdout else "")
    result.command = given_command
    result.real_command = command
    result.stderr = stderr.decode(errors="replace").strip() if stderr else ""
    result.return_code = process.returncode

    if result.failed:
        message = (
            f"local() encountered an error (return code {result.return_code}) "
            f"while executing '{given_command}'"
        )
        if _warn_only.get():
            if not _is_hidden("warnings"):
                logger.warning(message)
        else:
            logger.error(f"Fatal error: {message}")
            raise SystemExit(1)

    return result
