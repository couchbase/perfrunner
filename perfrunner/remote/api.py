"""Fabric 1-compatible remote execution API on top of ``perfrunner.remote.executor``.

Mirrors the Fabric 1 ``fabric.api`` surface perfrunner grew up on (``run``, ``get``,
``put``, ``cd``, ``shell_env``, ``settings``, ``execute``/``parallel``, ``env``/``output``
state), which kept the fabric3 -> modern-fabric migration an import flip rather than a
call-site rewrite. Key differences from Fabric 1, by design:

- targeting and options live in context variables, not process-global mutable state,
  so parallel execution uses threads instead of forked processes;
- connections are cached in a shared pool and reused across calls.
"""

import fnmatch
import os
import posixpath
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar, copy_context
from types import SimpleNamespace
from typing import Callable, Optional

from logger import logger
from perfrunner.helpers.shell import AttributeString
from perfrunner.helpers.shell import output as output  # Shared with local(); see shell.py
from perfrunner.remote.executor import (
    CommandTimeout,
    ConnectionPool,
    NetworkError,
    Session,
    SessionConfig,
)

__all__ = [
    "CommandTimeout",
    "NetworkError",
    "append",
    "cd",
    "disconnect_all",
    "env",
    "execute",
    "get",
    "hide",
    "output",
    "parallel",
    "put",
    "quiet",
    "run",
    "settings",
    "shell_env",
    "show",
    "state",
    "warn_only",
]

_host: ContextVar[str] = ContextVar("host", default="")
_gateway: ContextVar[str] = ContextVar("gateway", default="")
_cwd: ContextVar[str] = ContextVar("cwd", default="")
_env_vars: ContextVar[dict] = ContextVar("env_vars", default={})
_warn_only: ContextVar[bool] = ContextVar("warn_only", default=False)
_hidden: ContextVar[frozenset] = ContextVar("hidden", default=frozenset())
_shown: ContextVar[frozenset] = ContextVar("shown", default=frozenset())
_shell: ContextVar[str] = ContextVar("shell", default="")
_user: ContextVar[str] = ContextVar("user", default="")
_password: ContextVar[str] = ContextVar("password", default="")
_pool_size: ContextVar[int] = ContextVar("pool_size", default=0)


class _Env:
    """Global connection defaults + a live view of the current host (fabric.state.env compat).

    Written once at startup (RemoteHelper, remotestats); per-call overrides go through settings().
    ``host_string`` reflects the executing context, so parallel tasks reading it see their own host.
    """

    def __init__(self):
        self.user = None
        self.password = None
        self.shell = "/bin/bash -l -c"
        self.keepalive = 60
        self.timeout = 10
        self.disable_known_hosts = True
        self.use_ssh_config = False

    @property
    def host_string(self) -> str:
        return _host.get()

    @host_string.setter
    def host_string(self, value: str):
        _host.set(value)


env = _Env()
state = SimpleNamespace(env=env, output=output)

pool = ConnectionPool()


def _is_hidden(group: str) -> bool:
    shown = _shown.get()
    if group in shown:
        return False
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
def _set(var: ContextVar, value) -> Generator[None, None, None]:
    token = var.set(value)
    try:
        yield
    finally:
        var.reset(token)


def cd(path: str):
    """Prefix nested run()/get()/put() calls with a remote working directory."""
    current = _cwd.get()
    if current and not posixpath.isabs(path):
        path = posixpath.join(current, path)
    return _set(_cwd, path)


def shell_env(**env_vars: str):
    """Export the given environment variables for nested run() calls."""
    return _set(_env_vars, {**_env_vars.get(), **env_vars})


def warn_only():
    """Turn command failures into warnings instead of aborting."""
    return _set(_warn_only, True)


def hide(*groups: str):
    """Hide the given output groups ("running", "output", "warnings", "everything")."""
    return _set(_hidden, _hidden.get() | frozenset(groups))


def show(*groups: str):
    """Force the given output groups to be shown, overriding defaults and hide()."""
    return _set(_shown, _shown.get() | frozenset(groups))


@contextmanager
def quiet() -> Generator[None, None, None]:
    """Hide all output and turn failures into warnings."""
    with hide("everything"), warn_only():
        yield


_SETTINGS_VARS = {
    "host_string": _host,
    "gateway": _gateway,
    "warn_only": _warn_only,
    "shell": _shell,
    "user": _user,
    "password": _password,
    "pool_size": _pool_size,
}


@contextmanager
def settings(*context_managers, **kwargs) -> Generator[None, None, None]:
    """Apply nested context managers and/or fabric-style keyword overrides."""
    if unknown := set(kwargs) - set(_SETTINGS_VARS) - {"quiet"}:
        raise TypeError(f"Unsupported settings: {unknown}")
    with ExitStack() as stack:
        for manager in context_managers:
            stack.enter_context(manager)
        if kwargs.pop("quiet", False):
            stack.enter_context(quiet())
        for key, value in kwargs.items():
            stack.enter_context(_set(_SETTINGS_VARS[key], value))
        yield


def _session_config() -> SessionConfig:
    return SessionConfig(
        user=_user.get() or env.user,
        password=_password.get() or env.password,
        use_ssh_config=env.use_ssh_config,
        disable_known_hosts=env.disable_known_hosts,
        connect_timeout=env.timeout,
        keepalive=env.keepalive,
    )


def _current_session() -> Session:
    host = _host.get()
    if not host:
        raise RuntimeError("No host set: use settings(host_string=...) or execute(..., hosts=[])")
    user = None
    if "@" in host:
        user, host = host.rsplit("@", 1)
    config = _session_config()
    if user:
        config.user = user
    return pool.session(host, config, gateway=_gateway.get() or None)


def _shell_escape(command: str) -> str:
    # Intentionally byte-for-byte Fabric 1's _shell_escape, including its quirk with already-escaped
    # input. Callers that need different escaping pass shell_escape=False.
    for char in ('"', "$", "`"):
        command = command.replace(char, f"\\{char}")
    return command


def run(
    command: str,
    quiet: bool = False,
    warn_only: bool = False,
    pty: bool = True,
    timeout: Optional[int] = None,
    shell_escape: bool = True,
) -> AttributeString:
    """Run a command on the current host, mirroring Fabric 1 ``run()`` semantics.

    The command is wrapped in a login shell (``env.shell``) after applying the active
    ``cd()``/``shell_env()`` prefixes. Failures abort with ``SystemExit`` unless running
    under ``warn_only``/``quiet``.
    """
    given_command = command
    if env_vars := _env_vars.get():
        exports = " ".join(f'{key}="{value}"' for key, value in env_vars.items())
        command = f"export {exports} && {command}"
    if cwd := _cwd.get():
        command = f"cd {cwd} && {command}"

    shell = _shell.get() or env.shell
    escaped = _shell_escape(command) if shell_escape else command
    real_command = f'{shell} "{escaped}"'

    session = _current_session()
    hide_all = quiet
    if not hide_all and not _is_hidden("running"):
        logger.info(f"[{session.host}] run: {given_command}")

    with session.channels:  # Cap concurrent channels per connection (sshd MaxSessions)
        raw = session.run_raw(real_command, pty=pty, timeout=timeout)

    result = AttributeString(raw.stdout.strip())
    result.command = given_command
    result.real_command = real_command
    result.stderr = raw.stderr.strip()
    result.return_code = raw.return_code

    if not hide_all and not _is_hidden("output") and result:
        logger.info(f"[{session.host}] out: {result}")
    if not hide_all and not _is_hidden("stderr") and result.stderr:
        # Separate stream only with pty=False; pty merges stderr into stdout
        logger.info(f"[{session.host}] err: {result.stderr}")

    if result.failed:
        message = (
            f"run() received nonzero return code {result.return_code} "
            f"while executing '{given_command}' on {session.host}"
        )
        if result.stderr:
            message += f". stderr: {result.stderr}"
        if warn_only or quiet or _warn_only.get():
            if not hide_all and not _is_hidden("warnings"):
                logger.warning(message)
        else:
            logger.error(f"Fatal error: {message}")
            raise SystemExit(1)

    return result


def _resolve_remote(path: str) -> str:
    if cwd := _cwd.get():
        if not posixpath.isabs(path):
            return posixpath.join(cwd, path)
    return path


def _expand_globs(session: Session, remote_path: str) -> list[str]:
    if not any(char in remote_path for char in "*?["):
        return [remote_path]
    directory, pattern = posixpath.split(remote_path)
    return [
        posixpath.join(directory, name) if directory else name
        for name in session.listdir(directory)
        if fnmatch.fnmatch(name, pattern)
    ]


def _download_file(session: Session, remote_file: str, local_path: str, downloaded: list[str]):
    if local_path.endswith(os.sep) or os.path.isdir(local_path):
        local_file = os.path.join(local_path, posixpath.basename(remote_file))
    else:
        local_file = local_path
    if directory := os.path.dirname(local_file):
        os.makedirs(directory, exist_ok=True)
    session.download(remote_file, local_file)
    downloaded.append(local_file)


# SFTP exposes no inode numbers for symlink-cycle detection, so bound the recursion
# instead: a loop fails fast with a clear error rather than a deep RecursionError.
MAX_TREE_DEPTH = 64


def _download_tree(session: Session, remote_dir: str, local_dir: str, downloaded: list[str],
                   depth: int = 0):
    if depth > MAX_TREE_DEPTH:
        raise RuntimeError(f"Remote directory tree deeper than {MAX_TREE_DEPTH} levels "
                           f"at {remote_dir} (symlink loop?)")
    os.makedirs(local_dir, exist_ok=True)
    for name in session.listdir(remote_dir):
        remote_entry = posixpath.join(remote_dir, name)
        if session.isdir(remote_entry):
            _download_tree(session, remote_entry, os.path.join(local_dir, name), downloaded,
                           depth=depth + 1)
        else:
            _download_file(session, remote_entry, os.path.join(local_dir, name), downloaded)


def get(remote_path: str, local_path: Optional[str] = None) -> list[str]:
    """Download files, supporting globs and directories like Fabric 1 ``get()``.

    Without ``local_path``, Fabric 1's default layout applies: a single-file download
    lands at ``./<host>/<basename>``, while glob/directory downloads keep the full remote path under
    ``./<host>/`` to avoid collisions.
    """
    session = _current_session()
    is_glob = any(char in remote_path for char in "*?[")
    remote_path = _resolve_remote(remote_path)

    downloaded: list[str] = []
    matches = _expand_globs(session, remote_path)
    for match in matches:
        if local_path is None:
            if not is_glob and len(matches) == 1 and not session.isdir(match):
                target = os.path.join(session.host, posixpath.basename(match))
            else:
                target = os.path.join(session.host, match.lstrip("/"))
        else:
            target = local_path
        if session.isdir(match):
            if target.endswith(os.sep) or os.path.isdir(target):
                target = os.path.join(target, posixpath.basename(match))
            _download_tree(session, match, target, downloaded)
        else:
            _download_file(session, match, target, downloaded)
    return downloaded


def _upload_tree(session: Session, local_dir: str, remote_dir: str, uploaded: list[str]):
    session.makedirs(remote_dir)
    for name in sorted(os.listdir(local_dir)):
        local_entry = os.path.join(local_dir, name)
        remote_entry = posixpath.join(remote_dir, name)
        if os.path.isdir(local_entry):
            _upload_tree(session, local_entry, remote_entry, uploaded)
        else:
            session.upload(local_entry, remote_entry)
            uploaded.append(remote_entry)


def put(local_path: str, remote_path: str) -> list[str]:
    """Upload a file or a directory tree, like Fabric 1 ``put()``."""
    session = _current_session()
    remote_path = _resolve_remote(remote_path)

    uploaded: list[str] = []
    if os.path.isdir(local_path):
        remote_dir = posixpath.join(remote_path, os.path.basename(local_path.rstrip(os.sep)))
        _upload_tree(session, local_path, remote_dir, uploaded)
    else:
        # Fabric 1 stat'ed the remote side: uploading a file to an existing directory
        # (with or without a trailing slash) lands inside it.
        if remote_path.endswith("/") or session.isdir(remote_path):
            remote_path = posixpath.join(remote_path, os.path.basename(local_path))
        session.upload(local_path, remote_path)
        uploaded.append(remote_path)
    return uploaded


def append(filename: str, text: str, use_sudo: bool = False):
    """Append a line to a remote file unless it is already present (fabric contrib compat)."""
    sudo = "sudo " if use_sudo else ""
    run(
        f"{sudo}grep -qF -- '{text}' {filename} || "
        f"echo '{text}' | {sudo}tee -a {filename} > /dev/null",
        warn_only=True,
    )


def parallel(task: Callable) -> Callable:
    """Mark a task for parallel execution by execute() (fabric1-compatible no-op wrapper)."""
    task.parallel = True
    return task


def _call_on_host(task: Callable, host: str, args: tuple, kwargs: dict):
    with settings(host_string=host):
        return task(*args, **kwargs)


def execute(task: Callable, *args, hosts: Optional[list[str]] = None, **kwargs) -> dict:
    """Run a task once per host, in threads if the task is marked with parallel().

    Returns ``{host: task return value}``. In parallel mode every host runs to
    completion; each failure is logged and the first one is re-raised with its
    original type (callers catch e.g. CommandTimeout across execute()).
    """
    hosts = list(hosts or ([_host.get()] if _host.get() else []))
    if not hosts:
        raise RuntimeError("execute() requires hosts")

    results: dict = {}
    if getattr(task, "parallel", False) and len(hosts) > 1:
        max_workers = min(_pool_size.get() or len(hosts), len(hosts))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                host: executor.submit(copy_context().run, _call_on_host, task, host, args, kwargs)
                for host in hosts
            }
            errors: dict = {}
            for host, future in futures.items():
                try:
                    results[host] = future.result()
                except (Exception, SystemExit) as e:  # run() aborts raise SystemExit
                    errors[host] = e
            if errors:
                for host, error in errors.items():
                    logger.error(f"Parallel task failed on {host}: {error!r}")
                raise next(iter(errors.values()))
    else:
        for host in hosts:
            results[host] = _call_on_host(task, host, args, kwargs)
    return results


def disconnect_all():
    """Close all pooled connections (call at teardown)."""
    pool.close_all()
