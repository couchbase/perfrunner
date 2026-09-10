"""Celery task abort handling; spawns real child processes and sends real signals."""

import contextlib
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

import psutil

# perfrunner.helpers.worker configures celery when it is imported, and refuses to load unless it
# knows which kind of worker it is configuring. Declare one before importing it. Importing only
# updates celery's configuration; it does not connect to a broker or start anything.
os.environ.setdefault("WORKER_TYPE", "local")

from perfrunner.helpers.worker import (  # noqa: E402
    TASK_PIDFILE_DIR,
    LocalWorkerManager,
    RemoteWorkerManager,
    store_pid,
)


def fake_task_result(
    task_id: str, members: list = None, on_get: Callable = None
) -> SimpleNamespace:
    """Build a stand-in for a celery AsyncResult, or for a GroupResult if members are given.

    `on_get` stands in for what waiting on the real result would do, and is passed the timeout it
    was called with. By default it returns at once, as a task that has already finished would.
    """

    def get(timeout=None, propagate=True):
        if on_get is not None:
            return on_get(timeout)

    return SimpleNamespace(id=task_id, results=members, get=get)


# A task process shaped like spring: it traps SIGTERM and asks its worker child to stop, and the
# worker dumps its stats on the way out. The worker leaves SIGTERM at its default disposition,
# exactly as spring's worker processes do, so signalling the worker directly kills it before it can
# dump anything. Whether the stats file gets written is therefore the difference between a graceful
# shutdown and an abrupt kill.
GRACEFUL_TASK = """
import os, signal, sys, time

ready_file, stop_file, stats_file = sys.argv[1], sys.argv[2], sys.argv[3]

if os.fork() == 0:
    deadline = time.time() + 60
    while not os.path.exists(stop_file):
        if time.time() > deadline:
            os._exit(1)
        time.sleep(0.05)
    open(stats_file, "w").write("stats dumped")
    os._exit(0)

def shut_down(signum, frame):
    open(stop_file, "w").write("stop")
    os.wait()
    sys.exit(0)

signal.signal(signal.SIGTERM, shut_down)
open(ready_file, "w").write("ready")
time.sleep(60)
"""

# A child process which ignores SIGTERM entirely, to exercise the escalation path
STUBBORN_CHILD = """
import signal, sys, time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(sys.argv[1], "w").write("ready")
time.sleep(60)
"""


class CeleryTaskAbortTest(TestCase):
    """Cover aborting locally running celery tasks.

    The celery plumbing itself (that a task really gets submitted, run and revoked) needs a broker
    and a running worker, so it isn't covered here. What is covered is everything perfrunner does
    around it: recording task PIDs, resolving them back to processes, and terminating those
    processes.
    """

    def setUp(self):
        # Task pidfiles are written relative to the working directory, so run in a scratch one
        self._cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._children = []

    def tearDown(self):
        for child in self._children:
            # Kill the whole tree: the task processes here fork worker children of their own
            try:
                for grandchild in psutil.Process(child.pid).children(recursive=True):
                    with contextlib.suppress(psutil.Error):
                        grandchild.kill()
            except psutil.Error:
                pass
            try:
                child.kill()
                child.wait(timeout=10)
            except Exception:
                pass
        os.chdir(self._cwd)
        self._tmpdir.cleanup()

    def start_child(self, source: str, *args) -> subprocess.Popen:
        """Start a child process from the given source and wait until it is ready to be signalled.

        Waiting matters: signalling before the child has installed its handler would make a
        graceful shutdown look like an abrupt one.
        """
        ready_file = Path(f"ready-{len(self._children)}")
        child = subprocess.Popen(
            [sys.executable, "-c", source, str(ready_file), *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._children.append(child)
        deadline = time.time() + 30
        while not ready_file.exists() and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(ready_file.exists(), "child process did not start")
        return child

    @staticmethod
    def process_stopped(pid: int) -> bool:
        """Whether the process has stopped running.

        A process which has exited but not yet been reaped counts as stopped, since which of us
        reaps it depends on timing we don't control.
        """
        try:
            process = psutil.Process(pid)
            return not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return True

    @classmethod
    def wait_until_stopped(cls, pid: int, timeout: int = 15) -> bool:
        """Whether the process stops running within the timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cls.process_stopped(pid):
                return True
            time.sleep(0.05)
        return False

    @staticmethod
    def write_pidfile(task_id: str, pid: int):
        pidfile = Path(TASK_PIDFILE_DIR) / f"{task_id}.pid"
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(pid))
        return pidfile

    @staticmethod
    def local_worker_manager(terminate_timeout: int = 5) -> LocalWorkerManager:
        """Build a LocalWorkerManager without the celery worker set up done by __init__."""
        manager = LocalWorkerManager.__new__(LocalWorkerManager)
        manager.fg_async_results = []
        manager.bg_async_results = []
        manager._aborted_results = []
        manager.TASK_TERMINATE_TIMEOUT = terminate_timeout
        return manager

    def test_store_pid_writes_pidfile_while_task_runs(self):
        observed = {}

        @store_pid
        def task():
            pidfile = Path(TASK_PIDFILE_DIR) / "task-1.pid"
            observed["exists"] = pidfile.exists()
            observed["pid"] = pidfile.read_text()

        task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        self.assertTrue(observed["exists"])
        self.assertEqual(observed["pid"], str(os.getpid()))

    def test_store_pid_removes_pidfile_when_task_finishes(self):
        @store_pid
        def task():
            pass

        task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        # A leftover pidfile would let a recycled PID be signalled by mistake later on
        self.assertFalse((Path(TASK_PIDFILE_DIR) / "task-1.pid").exists())

    def test_store_pid_removes_pidfile_when_task_raises(self):
        @store_pid
        def task():
            raise RuntimeError("task failed")

        with self.assertRaises(RuntimeError):
            task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        self.assertFalse((Path(TASK_PIDFILE_DIR) / "task-1.pid").exists())

    def test_task_ids_of_single_result(self):
        self.assertEqual(LocalWorkerManager._task_ids(fake_task_result("task-1")), ["task-1"])

    def test_task_ids_of_group_result(self):
        # A GroupResult's own id is a group id, which no task writes a pidfile for, so the ids of
        # its members are what we need
        group = fake_task_result(
            "group-1", members=[fake_task_result("task-1"), fake_task_result("task-2")]
        )

        self.assertEqual(LocalWorkerManager._task_ids(group), ["task-1", "task-2"])

    def test_task_process_of_task_that_never_started(self):
        # No pidfile at all: the task was queued but never picked up by a worker
        self.assertIsNone(LocalWorkerManager._task_process("task-1"))

    def test_task_process_of_finished_task(self):
        # A pidfile naming a PID that is gone: the task already finished. This must not raise,
        # because tasks are aborted on successful runs too.
        child = self.start_child(STUBBORN_CHILD)
        pid = child.pid
        child.kill()
        child.wait(timeout=10)
        self.write_pidfile("task-1", pid)

        self.assertIsNone(LocalWorkerManager._task_process("task-1"))

    def test_task_process_of_running_task(self):
        child = self.start_child(STUBBORN_CHILD)
        self.write_pidfile("task-1", child.pid)

        process = LocalWorkerManager._task_process("task-1")

        self.assertIsNotNone(process)
        self.assertEqual(process.pid, child.pid)

    def test_abort_task_lets_the_task_shut_down_its_workers_gracefully(self):
        # Spring shuts its worker processes down from its own SIGTERM handler so that they get to
        # dump their stats, so only the task process itself may be signalled. Signalling its
        # workers as well kills them before they can dump anything.
        stats_file = Path("worker-stats")
        child = self.start_child(GRACEFUL_TASK, "stop-file", str(stats_file))
        self.write_pidfile("task-1", child.pid)
        manager = self.local_worker_manager()

        manager._abort_task(fake_task_result("task-1"))

        self.assertTrue(self.wait_until_stopped(child.pid), "task process was left running")
        self.assertTrue(
            stats_file.exists(), "worker process was killed before it could dump its stats"
        )
        self.assertEqual(stats_file.read_text(), "stats dumped")

    def test_abort_all_tasks_clears_the_result_lists(self):
        manager = self.local_worker_manager()
        manager.fg_async_results = [fake_task_result("task-1")]
        manager.bg_async_results = [fake_task_result("task-2")]

        manager.abort_all_tasks()

        # Aborted tasks are no longer tracked, so a later abort can't re-signal them
        self.assertEqual(manager.fg_async_results, [])
        self.assertEqual(manager.bg_async_results, [])

    def test_task_process_removes_the_pidfile(self):
        # A task that is killed outright never runs the `finally` in store_pid, so the reader has
        # to clean up instead - otherwise a recycled PID could be signalled later by mistake
        child = self.start_child(STUBBORN_CHILD)
        pidfile = self.write_pidfile("task-1", child.pid)

        self.assertIsNotNone(LocalWorkerManager._task_process("task-1"))

        self.assertFalse(pidfile.exists())

    def test_abort_all_tasks_waits_for_the_task_results(self):
        # Aborting must not return before the tasks have actually ended, because the caller goes on
        # to reconstruct measurements from the files the workers write while shutting down. The
        # task result is what signals that: the process running the task is a celery pool worker,
        # which is long-lived and does not exit when the task ends.
        waited = []
        manager = self.local_worker_manager()
        manager.fg_async_results = [
            fake_task_result("task-1", on_get=lambda timeout: waited.append("task-1"))
        ]
        manager.bg_async_results = [
            fake_task_result("task-2", on_get=lambda timeout: waited.append("task-2"))
        ]

        manager.abort_all_tasks()

        self.assertEqual(waited, ["task-1", "task-2"])

    def test_abort_all_tasks_does_not_wait_forever(self):
        # Bounded, so that a task which ignores SIGTERM can't stall teardown indefinitely
        def never_finishes(timeout):
            # Stand in for a task that outlives the wait: celery's `get` blocks for the timeout it
            # was given and then raises, so an unbounded wait would block far longer
            time.sleep(timeout if timeout is not None else 30)
            raise TimeoutError("task is still running")

        manager = self.local_worker_manager(terminate_timeout=1)
        manager.fg_async_results = [fake_task_result("task-1", on_get=never_finishes)]

        started = time.time()
        manager.abort_all_tasks()

        self.assertLess(time.time() - started, 10, "the wait was not bounded by the timeout")

    def test_abort_all_tasks_does_not_re_raise_task_failures(self):
        # An aborted task ending in failure is expected, and must not replace whatever the test was
        # actually doing (or failing with)
        def task_failed(*args, **kwargs):
            raise RuntimeError("task blew up")

        manager = self.local_worker_manager()
        manager.fg_async_results = [fake_task_result("task-1", on_get=task_failed)]

        manager.abort_all_tasks()  # must not raise

    def test_abort_all_tasks_is_best_effort(self):
        # Aborting runs on successful teardown too, so one task we cannot signal must not stop the
        # others from being signalled
        aborted = []

        class FailingWorkerManager(RemoteWorkerManager):
            def __init__(self):
                self.fg_async_results = [fake_task_result("task-1"), fake_task_result("task-2")]
                self.bg_async_results = [fake_task_result("task-3")]
                self._aborted_results = []

            def _abort_task(self, task_result):
                if task_result.id == "task-2":
                    raise RuntimeError("cannot signal this one")
                aborted.append(task_result.id)

        FailingWorkerManager().abort_all_tasks()

        self.assertEqual(aborted, ["task-1", "task-3"])
