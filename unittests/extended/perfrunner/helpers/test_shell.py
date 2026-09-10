"""perfrunner.helpers.shell; runs real local commands."""

import os
import tempfile
from unittest import TestCase

from perfrunner.helpers import shell
from perfrunner.remote import api


class LocalShellTest(TestCase):
    def test_capture_returns_stripped_stdout_with_attributes(self):
        with shell.quiet():
            result = shell.local("echo hello && echo oops >&2", capture=True)
        self.assertEqual(result, "hello")
        self.assertEqual(result.stdout, "hello")
        self.assertEqual(result.stderr, "oops")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)
        self.assertFalse(result.failed)

    def test_output_state_shared_with_remote_api(self):
        # RemoteHelper sets state.output.stdout/running from its verbose flag; that
        # must control local() too, like fabric.state.output did (Fabric 1 parity).
        self.assertIs(api.output, shell.output)
        self.assertIs(api.state.output, shell.output)

    def test_non_verbose_discards_output_and_echo(self):
        saved_running, saved_stdout = shell.output.running, shell.output.stdout
        try:
            shell.output.running = shell.output.stdout = False
            self.assertTrue(shell._is_hidden("running"))
            self.assertTrue(shell._is_hidden("output"))
            result = shell.local("true")  # passthrough mode routes to devnull, no echo
            self.assertEqual(result.return_code, 0)
        finally:
            shell.output.running, shell.output.stdout = saved_running, saved_stdout
        self.assertFalse(shell._is_hidden("output"))

    def test_stderr_stays_visible_in_non_verbose_mode(self):
        # Fabric 1 keyed the streams separately and RemoteHelper only disables stdout,
        # so error text from local commands must survive non-verbose runs.
        saved = shell.output.stdout
        try:
            shell.output.stdout = False
            self.assertTrue(shell._is_hidden("output"))
            self.assertFalse(shell._is_hidden("stderr"))
        finally:
            shell.output.stdout = saved
        with shell.hide("output"):  # per-call hide("output") covers both streams
            self.assertTrue(shell._is_hidden("stderr"))

    def test_passthrough_mode_inherits_stdio(self):
        # capture=False with nothing hidden: child stdout/stderr inherit from the
        # parent and the result string is empty, but attributes are still populated.
        result = shell.local("true")
        self.assertEqual(result, "")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)

    def test_failure_aborts_by_default(self):
        with self.assertRaises(SystemExit):
            with shell.hide("everything"):
                shell.local("exit 1", capture=True)

    def test_warn_only_returns_failed_result(self):
        with shell.quiet():
            result = shell.local("exit 7", capture=True)
        self.assertEqual(result.return_code, 7)
        self.assertTrue(result.failed)
        self.assertFalse(result.succeeded)

    def test_settings_with_hide_and_warn_only(self):
        with shell.settings(shell.hide("output", "warnings"), warn_only=True):
            result = shell.local("exit 3")
        self.assertEqual(result.return_code, 3)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with shell.settings(host_string="node-1"):
                pass

    def test_lcd_nests_relative_paths(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sub_dir = os.path.join(tmp_dir, "sub")
            os.mkdir(sub_dir)
            with shell.quiet(), shell.lcd(tmp_dir), shell.lcd("sub"):
                result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(sub_dir))

    def test_lcd_restores_previous_directory(self):
        with shell.quiet():
            with shell.lcd("/"):
                pass
            result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(os.getcwd()))

    def test_shell_env_exports_variables(self):
        with shell.quiet(), shell.shell_env(FOO="bar", BAZ="qux"):
            result = shell.local("echo $FOO-$BAZ", capture=True)
        self.assertEqual(result, "bar-qux")

    def test_shell_executable_override(self):
        with shell.quiet():
            result = shell.local("echo $0", capture=True, shell="/bin/bash")
        self.assertEqual(result, "/bin/bash")

    def test_command_attributes_record_real_command(self):
        with shell.quiet(), shell.lcd("/tmp"), shell.shell_env(FOO="bar"):
            result = shell.local("true", capture=True)
        self.assertEqual(result.command, "true")
        self.assertIn("cd /tmp", result.real_command)
        self.assertIn('export FOO="bar"', result.real_command)
