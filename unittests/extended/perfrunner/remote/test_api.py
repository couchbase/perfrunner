"""Remote dispatch layer, driven by a fake session so no SSH connection is made."""

import os
import tempfile
import threading
import time
from pathlib import Path
from unittest import TestCase

from perfrunner.remote import api, executor


class RemoteApiTest(TestCase):
    def setUp(self):
        self.created = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            session.config = config
            session.gateway = gateway
            responses = self.scripted.get(host, {})
            session.responses.update(responses)
            self.created[host] = session
            return session

        self.scripted = {}
        self._saved_pool = api.pool
        api.pool = executor.ConnectionPool(factory)
        self._saved_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._saved_cwd)
        self._tmp.cleanup()
        api.pool = self._saved_pool

    def test_run_wraps_command_in_login_shell(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("echo hello")
        command, kwargs = self.created["node-1"].commands[0]
        self.assertEqual(command, '/bin/bash -l -c "echo hello"')
        self.assertTrue(kwargs["pty"])

    def test_run_escapes_shell_characters(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run('echo "$HOME" `id`')
            api.run('echo "$HOME"', shell_escape=False)
        escaped, _ = self.created["node-1"].commands[0]
        raw, _ = self.created["node-1"].commands[1]
        self.assertEqual(escaped, '/bin/bash -l -c "echo \\"\\$HOME\\" \\`id\\`"')
        self.assertEqual(raw, '/bin/bash -l -c "echo "$HOME""')

    def test_cd_and_shell_env_prefixes(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            with api.cd("/tmp/perfrunner"), api.cd("worker"), api.shell_env(GOGC="300"):
                api.run("make")
        command, _ = self.created["node-1"].commands[0]
        self.assertEqual(
            command, '/bin/bash -l -c "cd /tmp/perfrunner/worker && export GOGC=\\"300\\" && make"'
        )

    def test_connection_reuse_across_calls(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("true")
            api.run("true")
        self.assertEqual(len(self.created), 1)
        self.assertEqual(len(self.created["node-1"].commands), 2)

    def test_execute_parallel_returns_dict_by_host(self):
        hosts = ["h1", "h2", "h3"]
        wrapped = '/bin/bash -l -c "hostname"'
        for host in hosts:
            self.scripted[host] = {wrapped: executor.RunResult(f"out-{host}", "", 0)}

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True))

        results = api.execute(task, hosts=hosts)
        self.assertEqual(results, {host: f"out-{host}" for host in hosts})
        for host in hosts:
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_parallel_all_hosts_complete_despite_failure(self):
        # One failing host must not discard the other hosts' work, and the
        # original exception type must surface (not a generic wrapper).
        wrapped = '/bin/bash -l -c "hostname"'
        self.scripted = {
            "h1": {wrapped: executor.RunResult("out-h1", "", 0)},
            "h2": {wrapped: executor.CommandTimeout("timed out")},
            "h3": {wrapped: executor.RunResult("out-h3", "", 0)},
        }

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True, timeout=5))

        with self.assertRaises(executor.CommandTimeout):
            api.execute(task, hosts=["h1", "h2", "h3"])
        for host in ("h1", "h2", "h3"):
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_serial_lambda(self):
        results = api.execute(lambda: api.run("true", quiet=True), hosts=["h1"])
        self.assertIn("h1", results)

    def test_run_failure_aborts_by_default(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with self.assertRaises(SystemExit):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("false")

    def test_run_failure_with_warn_only(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with api.settings(api.hide("everything"), host_string="node-1"):
            result = api.run("false", warn_only=True)
        self.assertEqual(result.return_code, 1)
        self.assertTrue(result.failed)

    def test_run_failure_reports_stderr(self):
        wrapped = '/bin/bash -l -c "systemctl restart couchbase-server"'
        self.scripted["n1"] = {
            wrapped: executor.RunResult("", "Job for couchbase-server failed", 1)
        }
        with self.assertLogs(level="WARNING") as logs:
            with api.settings(api.hide("running", "output"), host_string="n1"):
                result = api.run("systemctl restart couchbase-server", warn_only=True, pty=False)
        self.assertEqual(result.stderr, "Job for couchbase-server failed")
        self.assertTrue(any("Job for couchbase-server failed" in line for line in logs.output))

    def test_failed_command_reports_output_even_when_hidden(self):
        # Perfrunner runs non-verbose, so output is hidden. A command that aborts the run must
        # still print what it said, or a Jenkins log has an exit code and nothing else.
        wrapped = '/bin/bash -l -c "cbbackupmgr restore"'
        self.scripted["n1"] = {
            wrapped: executor.RunResult("Error restoring cluster: remapping a bucket", "", 1)
        }
        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(SystemExit):
                with api.settings(api.hide("everything"), host_string="n1"):
                    api.run("cbbackupmgr restore")
        report = "\n".join(logs.output)
        self.assertIn("Standard output", report)
        self.assertIn("Error restoring cluster: remapping a bucket", report)

    def test_network_error_follows_warn_only(self):
        # A connection blip on a best-effort cleanup call must not fail the build, but an
        # unguarded call still has to raise: linux.py::is_up depends on catching it.
        wrapped = '/bin/bash -l -c "rm -rf results/latest"'
        self.scripted["n1"] = {wrapped: executor.NetworkError("Connection reset by peer")}
        with api.settings(api.hide("running", "output"), host_string="n1"):
            result = api.run("rm -rf results/latest", warn_only=True)
        self.assertTrue(result.failed)
        self.assertIn("Connection reset by peer", result.stderr)

        with self.assertRaises(executor.NetworkError):
            with api.settings(api.hide("everything"), host_string="n1"):
                api.run("rm -rf results/latest")

    def test_handled_failure_reports_output_when_it_was_not_logged(self):
        # A pty merges stderr into stdout, so a warn_only failure would otherwise warn with
        # a return code and no reason on a non-verbose run.
        wrapped = '/bin/bash -l -c "cbbackupmgr restore"'
        self.scripted["n1"] = {wrapped: executor.RunResult("Error: archive not found", "", 1)}
        with self.assertLogs(level="WARNING") as logs:
            with api.settings(api.hide("running", "output"), host_string="n1"):
                api.run("cbbackupmgr restore", warn_only=True)
        self.assertTrue(any("Error: archive not found" in line for line in logs.output))

    def test_network_error_marks_the_session_for_a_probe(self):
        wrapped = '/bin/bash -l -c "true"'
        self.scripted["n1"] = {wrapped: executor.NetworkError("Connection reset by peer")}
        with api.settings(api.hide("everything"), host_string="n1"):
            api.run("true", warn_only=True)
        self.assertTrue(self.created["n1"].needs_probe)

    def test_a_refused_channel_leaves_the_session_alone(self):
        # sshd MaxSessions: the transport is fine, so the connection other threads are using
        # must not be queued for a liveness probe.
        wrapped = '/bin/bash -l -c "true"'
        self.scripted["n1"] = {wrapped: executor.ChannelError("open failed")}
        with api.settings(api.hide("everything"), host_string="n1"):
            result = api.run("true", warn_only=True)
        self.assertTrue(result.failed)
        self.assertFalse(self.created["n1"].needs_probe)

    def test_command_timeout_propagates(self):
        self.scripted["node-1"] = {
            '/bin/bash -l -c "sleep 100"': executor.CommandTimeout("timed out")
        }
        with self.assertRaises(executor.CommandTimeout):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("sleep 100", timeout=10)

    def test_get_glob_with_default_host_layout(self):
        self.scripted["10.1.1.1"] = {}
        with api.settings(api.hide("everything"), host_string="10.1.1.1"):
            session = api.pool.session("10.1.1.1", executor.SessionConfig())
            session.files = {"/tmp/aaa.zip": "x", "/tmp/bbb.zip": "y", "/tmp/keep.log": "z"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(session.downloads),
            [
                ("/tmp/aaa.zip", os.path.join("10.1.1.1", "tmp", "aaa.zip")),
                ("/tmp/bbb.zip", os.path.join("10.1.1.1", "tmp", "bbb.zip")),
            ],
        )
        self.assertEqual(len(downloaded), 2)

    def test_get_relative_path_uses_cd(self):
        with api.settings(api.hide("everything"), host_string="w1"):
            session = api.pool.session("w1", executor.SessionConfig())
            session.files = {"/worker/perfrunner/worker_1.log": "log"}
            with api.cd("/worker/perfrunner"):
                api.get("worker_*.log", local_path="celery/")
        self.assertEqual(
            session.downloads,
            [("/worker/perfrunner/worker_1.log", os.path.join("celery", "worker_1.log"))],
        )

    def test_get_single_file_default_lands_at_host_slash_basename(self):
        # Contract with the debug flow: a bare get() of one file must land exactly one level deep.
        # Fabric 1 collapsed %(path)s to the basename for single-file downloads.
        with api.settings(api.hide("everything"), host_string="10.1.1.3"):
            session = api.pool.session("10.1.1.3", executor.SessionConfig())
            session.files = {"/tmp/abc123.zip": "z"}
            downloaded = api.get("/tmp/abc123.zip")
        self.assertEqual(downloaded, [os.path.join("10.1.1.3", "abc123.zip")])

    def test_get_glob_default_keeps_full_path(self):
        # Glob downloads keep the full remote path under <host>/ to avoid collisions.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.files = {"/tmp/a.zip": "a", "/tmp/b.zip": "b"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(downloaded),
            [os.path.join("h1", "tmp", "a.zip"), os.path.join("h1", "tmp", "b.zip")],
        )

    def test_put_directory_recursively(self):
        os.makedirs("inbox/sub")
        Path("inbox/chain.pem").write_text("pem")
        Path("inbox/sub/node.key").write_text("key")
        with api.settings(api.hide("everything"), host_string="n1"):
            uploaded = api.put("inbox", "/opt/couchbase/var/lib/couchbase")
        session = self.created["n1"]
        self.assertIn(
            ("inbox/chain.pem", "/opt/couchbase/var/lib/couchbase/inbox/chain.pem"),
            [(os.path.relpath(local), remote) for local, remote in session.uploads],
        )
        self.assertIn("/opt/couchbase/var/lib/couchbase/inbox/sub", session.dirs)
        self.assertEqual(len(uploaded), 2)

    def test_get_directory_downloads_tree_recursively(self):
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.dirs = {"/data", "/data/a", "/data/b"}
            session.files = {"/data/f0": "0", "/data/a/f1": "1", "/data/b/f2": "2"}
            downloaded = api.get("/data", local_path="out/")
        self.assertEqual(
            sorted(session.downloads),
            [
                ("/data/a/f1", os.path.join("out", "data", "a", "f1")),
                ("/data/b/f2", os.path.join("out", "data", "b", "f2")),
                ("/data/f0", os.path.join("out", "data", "f0")),
            ],
        )
        self.assertEqual(sorted(downloaded), sorted(local for _, local in session.downloads))
        self.assertTrue(os.path.isdir(os.path.join("out", "data", "a")))

    def test_download_tree_depth_guard(self):
        # SFTP has no inode info for cycle detection; a symlink loop must fail fast
        # with a clear error instead of a RecursionError.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            path = ""
            for level in range(api.MAX_TREE_DEPTH + 2):
                path = f"{path}/d"
                session.dirs.add(path)
            with self.assertRaises(RuntimeError):
                api.get("/d")

    def test_concurrent_channels_capped_per_session(self):
        # Nested parallel decorators stack many threads on one pooled connection;
        # channel opens must be capped below sshd MaxSessions.
        counters = {"current": 0, "max": 0}
        guard = threading.Lock()

        def tracked_run_raw(command, pty=True, timeout=None):
            with guard:
                counters["current"] += 1
                counters["max"] = max(counters["max"], counters["current"])
            time.sleep(0.02)
            with guard:
                counters["current"] -= 1
            return executor.RunResult("", "", 0)

        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
        session.run_raw = tracked_run_raw

        def worker():
            with api.settings(api.hide("everything"), host_string="h1"):
                api.run("true", quiet=True)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertGreater(counters["max"], 1)
        self.assertLessEqual(counters["max"], executor.Session.MAX_CONCURRENT_CHANNELS)

    def test_put_file_into_existing_remote_directory(self):
        # Fabric 1 parity: put("root.pem", "<dir>") with an existing remote directory
        # (no trailing slash) must land <dir>/root.pem, not open the dir for write.
        Path("root.pem").write_text("pem")
        with api.settings(api.hide("everything"), host_string="c1"):
            session = api.pool.session("c1", executor.SessionConfig())
            session.dirs.add("/worker/perfrunner")
            uploaded = api.put("root.pem", "/worker/perfrunner")
        self.assertEqual(uploaded, ["/worker/perfrunner/root.pem"])
        self.assertEqual(session.uploads, [("root.pem", "/worker/perfrunner/root.pem")])

    def test_gateway_creates_jump_session(self):
        with api.settings(api.hide("everything"), host_string="kafka-1", gateway="jump-1"):
            api.run("true")
        self.assertIn("jump-1", self.created)
        self.assertIs(self.created["kafka-1"].gateway, self.created["jump-1"])

    def test_append_is_idempotent_grep(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x")
        command, _ = self.created["n1"].commands[0]
        self.assertIn("grep -qF -- 'export LD_LIBRARY_PATH=/x'", command)
        self.assertIn("| tee -a /opt/tomcat/bin/setenv.sh", command)
        self.assertNotIn("sudo", command)

    def test_append_with_use_sudo(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x", use_sudo=True)
        command, _ = self.created["n1"].commands[0]
        self.assertIn("sudo grep -qF", command)
        self.assertIn("| sudo tee -a /opt/tomcat/bin/setenv.sh", command)

    def test_state_aliases(self):
        self.assertIs(api.state.env, api.env)
        self.assertIs(api.state.output, api.output)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with api.settings(bogus=1):
                pass
