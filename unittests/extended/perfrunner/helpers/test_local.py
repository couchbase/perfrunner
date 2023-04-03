import io
import os
import shutil
import subprocess
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, skipUnless

from perfrunner.helpers import local


@skipUnless(shutil.which("ar") and shutil.which("unxz"), "needs binutils ar and xz-utils")
class ExtractCbTest(TestCase):
    """Cover `extract_cb` and the predicates its consumers use in place of calling it.

    Only the .deb path is exercised end to end: `rpm2cpio` is not installed everywhere the
    unit tests run, while `ar` and `tar` come with the toolchain the Makefile already needs.
    """

    TOOL = "cbbackupmgr"

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp.name)

    def _package(
        self, name: str = "couchbase.deb", payload: bytes = b"tools\n", member: str = ""
    ) -> Path:
        """Write a .deb whose data.tar.xz holds ./opt/couchbase/bin/<TOOL>, as a real one does."""
        package = Path(name)
        package.unlink(missing_ok=True)
        with tarfile.open("data.tar.xz", "w:xz") as tar:
            info = tarfile.TarInfo(member or f"./opt/couchbase/bin/{self.TOOL}")
            info.size = len(payload)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(payload))
        subprocess.run(["ar", "rc", name, "data.tar.xz"], check=True)
        os.remove("data.tar.xz")
        return package

    @property
    def tool(self) -> Path:
        """The extracted tool every consumer of ./opt would go on to run."""
        return Path(local.CB_TOOLS_DIR, self.TOOL)

    def test_extracts_the_tools(self):
        self._package()

        self.assertTrue(local.extract_cb("couchbase.deb"))

        self.assertEqual(self.tool.read_bytes(), b"tools\n")
        self.assertTrue(local.cb_tools_available())

    def test_a_package_from_another_build_replaces_the_tools(self):
        """Nothing guards the extraction: the package just downloaded is the one that wins."""
        self._package(payload=b"build A\n")
        local.extract_cb("couchbase.deb")

        self._package(payload=b"build B\n")
        local.extract_cb("couchbase.deb")

        self.assertEqual(self.tool.read_bytes(), b"build B\n")

    def test_a_corrupt_package_reports_failure(self):
        Path("couchbase.deb").write_bytes(b"not an archive at all")

        self.assertFalse(local.extract_cb("couchbase.deb"))
        self.assertFalse(local.cb_tools_available())

    def test_a_package_without_the_tools_reports_failure(self):
        """Extraction can succeed on the wrong package: a dbgsym .deb unpacks under ./usr."""
        self._package(member="./usr/lib/debug/couchbase-server.debug")

        self.assertFalse(local.extract_cb("couchbase.deb"))

        # The extraction itself succeeded, so only the tools-dir check caught this
        self.assertTrue(Path("./usr/lib/debug/couchbase-server.debug").is_file())
        self.assertFalse(local.cb_tools_available())

    def test_a_missing_package_reports_failure(self):
        self.assertFalse(local.extract_cb("couchbase.deb"))

    def test_an_unsupported_package_type_reports_failure(self):
        """A Windows install downloads couchbase.exe, which there is nothing to unpack."""
        Path("couchbase.exe").write_bytes(b"MZ")

        self.assertFalse(local.extract_cb("couchbase.exe"))

    def test_require_cb_tools_aborts_only_when_the_tools_are_missing(self):
        with self.assertRaises(SystemExit):
            local.require_cb_tools()

        self._package()
        local.extract_cb("couchbase.deb")
        local.require_cb_tools()

    def test_cb_package_candidates(self):
        """Still used by the master client, which is handed a bare name to search for."""
        self.assertEqual(
            local.cb_package_candidates("couchbase"), ["couchbase.deb", "couchbase.rpm"]
        )
        self.assertEqual(local.cb_package_candidates("couchbase.rpm"), ["couchbase.rpm"])
        self.assertEqual(local.cb_package_candidates("couchbase.tar.gz"), [])
