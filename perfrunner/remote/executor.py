"""Transport layer for remote command execution.

A ``Session`` is the minimal executor interface the compat API (``perfrunner.remote.api``)
builds on: run a raw command string, transfer files, inspect the remote filesystem. Keeping this
interface small and dumb is deliberate. Command construction (cd/env prefixes, shell wrapping,
retries, logging) lives above it. Tool wrappers only need an executor with these primitives.

``SSHSession`` is backed by Fabric, imported lazily so that importing this module (and
everything built on it) does not load the SSH stack (fabric/paramiko/cryptography) until a
session is actually created. ``FakeSession`` is an in-memory double used by unit tests and
characterisation harnesses, which therefore run without the SSH stack at all.
"""

import os
import posixpath
import stat
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, NamedTuple, Optional


class CommandTimeout(Exception):
    """Raised when a remote command exceeds its timeout (fabric1-compatible name)."""


class NetworkError(Exception):
    """Raised on SSH connection or transport failures (fabric1-compatible name)."""


class RunResult(NamedTuple):
    stdout: str
    stderr: str
    return_code: int


@dataclass
class SessionConfig:
    """Connection parameters, sourced from ``api.env`` when a session is created."""

    user: Optional[str] = None
    password: Optional[str] = None
    use_ssh_config: bool = False
    disable_known_hosts: bool = True
    connect_timeout: int = 10
    keepalive: int = 60


class Session:
    """Minimal executor interface: raw command execution + file transfer primitives."""

    # Nested parallel decorators (e.g. @all_servers + @syncgateway_servers) make many
    # threads run commands over ONE pooled connection at once; sshd rejects channel
    # opens beyond MaxSessions (default 10, lower on some hosts). Fabric 1 forked and
    # reconnected per task so it never stacked channels; we cap them instead.
    MAX_CONCURRENT_CHANNELS = 4

    def __init__(self, host: str):
        self.host = host
        self.last_used = 0.0
        self.channels = threading.Semaphore(self.MAX_CONCURRENT_CHANNELS)

    def probe(self):
        """Cheap liveness round trip; raise NetworkError if the connection is dead."""

    def run_raw(self, command: str, pty: bool = True, timeout: Optional[int] = None) -> RunResult:
        raise NotImplementedError

    def listdir(self, path: str) -> list[str]:
        raise NotImplementedError

    def isdir(self, path: str) -> bool:
        raise NotImplementedError

    def download(self, remote_file: str, local_file: str):
        raise NotImplementedError

    def upload(self, local_file: str, remote_file: str):
        raise NotImplementedError

    def makedirs(self, path: str):
        raise NotImplementedError

    def is_active(self) -> bool:
        return True

    def close(self):
        pass


class SSHSession(Session):
    """SSH-backed session on Fabric with a persistent, keepalive'd connection."""

    def __init__(self, host: str, config: SessionConfig, gateway: Optional["SSHSession"] = None):
        super().__init__(host)
        import fabric  # Lazy: keep the SSH stack out of module import time

        connect_kwargs: dict = {"look_for_keys": True}
        if config.password:
            connect_kwargs["password"] = config.password

        fabric_config = fabric.Config(
            overrides={"load_ssh_configs": config.use_ssh_config},
        )
        self._conn = fabric.Connection(
            host,
            user=config.user,
            config=fabric_config,
            gateway=gateway._conn if gateway else None,
            connect_timeout=config.connect_timeout,
            connect_kwargs=connect_kwargs,
        )
        # Fabric's Connection already uses AutoAddPolicy with no known_hosts loaded, which is what
        # disable_known_hosts=True means. Strict mode restores paramiko's known-hosts verification.
        if not config.disable_known_hosts:
            import paramiko

            self._conn.client.load_system_host_keys()
            self._conn.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        self._keepalive = config.keepalive

    def _open(self):
        if self._conn.is_connected:
            return

        try:
            self._conn.open()
            self._conn.transport.set_keepalive(self._keepalive)
        except Exception as e:
            raise NetworkError(f"Failed to connect to {self.host}: {e}") from e

    def run_raw(self, command: str, pty: bool = True, timeout: Optional[int] = None) -> RunResult:
        from invoke.exceptions import CommandTimedOut
        from paramiko.ssh_exception import SSHException

        self._open()
        try:
            result = self._conn.run(
                command, pty=pty, timeout=timeout, warn=True, hide=True, in_stream=False
            )
        except CommandTimedOut as e:
            raise CommandTimeout(f"Command timed out after {timeout}s on {self.host}") from e
        except (EOFError, OSError, SSHException) as e:
            raise NetworkError(f"Connection to {self.host} failed: {e}") from e
        return RunResult(result.stdout, result.stderr, result.exited)

    def _sftp(self):
        self._open()
        return self._conn.sftp()

    def listdir(self, path: str) -> list[str]:
        return self._sftp().listdir(path or ".")

    def isdir(self, path: str) -> bool:
        try:
            return stat.S_ISDIR(self._sftp().stat(path).st_mode)
        except IOError:
            return False

    def download(self, remote_file: str, local_file: str):
        self._sftp().get(remote_file, local_file)

    def upload(self, local_file: str, remote_file: str):
        self._sftp().put(local_file, remote_file)

    def makedirs(self, path: str):
        sftp = self._sftp()
        parts = path.split("/")
        current = "/" if path.startswith("/") else ""
        for part in filter(None, parts):
            current = posixpath.join(current, part) if current else part
            try:
                sftp.stat(current)
            except IOError:
                sftp.mkdir(current)

    def is_active(self) -> bool:
        return self._conn.is_connected and self._conn.transport.is_active()

    def probe(self):
        """Open and close a throwaway channel to prove the transport is really alive.

        A NAT/firewall can silently drop an idle flow without the local transport noticing,
        so ``is_active()`` alone is not trustworthy after idle gaps.
        """
        transport = self._conn.transport if self._conn.is_connected else None
        if transport is None:
            return
        try:
            transport.open_session(timeout=10).close()
        except Exception as e:
            raise NetworkError(f"Pooled connection to {self.host} is dead: {e}") from e

    def close(self):
        if self._conn.is_connected:
            self._conn.close()


@dataclass
class FakeSession(Session):
    """In-memory session double for unit tests and characterisation.

    Commands are recorded in ``commands``; scripted results are looked up in ``responses``
    (exact command string -> RunResult or Exception). ``files`` maps remote paths to file
    content, and ``dirs`` is the set of remote directories.
    """

    host: str = "fake-host"
    commands: list[tuple[str, dict]] = field(default_factory=list)
    responses: dict[str, object] = field(default_factory=dict)
    files: dict[str, str] = field(default_factory=dict)
    dirs: set = field(default_factory=set)
    downloads: list[tuple[str, str]] = field(default_factory=list)
    uploads: list[tuple[str, str]] = field(default_factory=list)
    closed: bool = False
    last_used: float = 0.0
    probes: int = 0
    probe_error: Optional[Exception] = None
    probe_delay: float = 0.0
    active: bool = True

    def __post_init__(self):
        self.channels = threading.Semaphore(self.MAX_CONCURRENT_CHANNELS)

    def is_active(self) -> bool:
        return self.active

    def probe(self):
        self.probes += 1
        if self.probe_delay:
            time.sleep(self.probe_delay)
        if self.probe_error is not None:
            raise self.probe_error

    def run_raw(self, command: str, pty: bool = True, timeout: Optional[int] = None) -> RunResult:
        self.commands.append((command, {"pty": pty, "timeout": timeout}))
        response = self.responses.get(command, RunResult("", "", 0))
        if isinstance(response, Exception):
            raise response
        return response

    def listdir(self, path: str) -> list[str]:
        path = (path or ".").rstrip("/")
        entries = set()
        for known in list(self.files) + list(self.dirs):
            parent, name = posixpath.split(known)
            if parent == path:
                entries.add(name)
        return sorted(entries)

    def isdir(self, path: str) -> bool:
        return path.rstrip("/") in self.dirs

    def download(self, remote_file: str, local_file: str):
        self.downloads.append((remote_file, local_file))

    def upload(self, local_file: str, remote_file: str):
        self.uploads.append((local_file, remote_file))
        self.files[remote_file] = f"<uploaded from {local_file}>"

    def makedirs(self, path: str):
        self.dirs.add(path.rstrip("/"))

    def close(self):
        self.closed = True


class ConnectionPool:
    """Cache of one session per (host, user, gateway), shared across the process.

    A session idle for longer than ``PROBE_AFTER_IDLE`` seconds is probed with a real
    round trip before reuse and transparently replaced if dead. NATs can silently drop idle flows
    without the local transport noticing. In-flight failures are NOT retried: re-running a
    non-idempotent command is worse than surfacing the NetworkError, and matches fabric1 semantics.
    """

    PROBE_AFTER_IDLE = 120  # seconds

    def __init__(self, session_factory: Callable[..., Session] = SSHSession):
        self.session_factory = session_factory
        self._sessions: dict[tuple, Session] = {}
        # The pool-wide lock only guards the dicts. Slow work (probes, closes) runs under a per-key
        # lock so that one dead host's probe timeout cannot stall parallel checkouts of other host.
        self._lock = threading.Lock()
        self._key_locks: dict[tuple, threading.Lock] = defaultdict(threading.Lock)
        self._pid = os.getpid()

    def _discard_inherited_sessions(self):
        """Drop sessions inherited through fork() without closing them.

        Forked children inherit the parent's open SSH sockets. Reusing them makes two processes
        read/write one encrypted stream. The parent sees stray replies and the stream can corrupt.
        Closing them from the child would send SSH disconnects on the parent's socket, so the child
        simply forgets them and connects fresh. Per-key locks are also recreated as one held by a
        parent thread at fork time would stay locked forever in the child.
        """
        if self._pid != os.getpid():
            self._sessions.clear()
            self._key_locks = defaultdict(threading.Lock)
            self._pid = os.getpid()

    def _key_lock(self, key: tuple) -> threading.Lock:
        with self._lock:
            self._discard_inherited_sessions()
            return self._key_locks[key]

    def _checkout(
        self, key: tuple, create: Callable[[], Session], probe_idle: bool = True
    ) -> Session:
        """Return a live session for ``key``, probing/replacing under its own lock only."""
        with self._key_lock(key):
            with self._lock:
                session = self._sessions.get(key)
            if session is not None and session.is_active():
                if probe_idle and time.time() - session.last_used > self.PROBE_AFTER_IDLE:
                    try:
                        session.probe()
                    except NetworkError:
                        session.close()
                        session = None
                if session is not None:
                    session.last_used = time.time()
                    return session
            if session is not None:
                session.close()
            session = create()
            session.last_used = time.time()
            with self._lock:
                self._sessions[key] = session
            return session

    def session(self, host: str, config: SessionConfig, gateway: Optional[str] = None) -> Session:
        key = (host, config.user, gateway)
        return self._checkout(key, lambda: self._create(host, config, gateway))

    def _create(self, host: str, config: SessionConfig, gateway: Optional[str]) -> Session:
        gateway_session = None
        if gateway:
            # Nested checkout: host-key lock -> gateway-key lock, never the other way around,
            # so the ordering cannot deadlock. Gateway liveness is checked and dead sessions closed
            # by the same path as regular sessions.
            gateway_key = (gateway, config.user, None)
            gateway_session = self._checkout(
                gateway_key,
                lambda: self.session_factory(gateway, config, gateway=None),
                probe_idle=False,
            )
        return self.session_factory(host, config, gateway=gateway_session)

    def close_all(self):
        with self._lock:
            for session in self._sessions.values():
                session.close()
            self._sessions.clear()
            self._key_locks = defaultdict(threading.Lock)
