"""`cb_product` package-name matching; extended because importing install.py drags in paramiko."""

from argparse import Namespace
from collections import namedtuple
from types import SimpleNamespace
from unittest import TestCase, mock

from perfrunner.helpers.misc import create_build_tuple
from perfrunner.settings import CBProduct
from perfrunner.utils import install
from perfrunner.utils.install import CouchbaseInstaller

CB_ENTERPRISE_DEB = "couchbase-server-enterprise_8.0.0-1234-linux_amd64.deb"
CB_ENTERPRISE_RELEASE_DEB = "couchbase-server-enterprise_8.0.0-linux_amd64.deb"
CB_ENTERPRISE_RPM = "couchbase-server-enterprise-8.0.0-1234-linux.x86_64.rpm"
CB_COMMUNITY_DEB = "couchbase-server-community_7.6.0-1234-linux_amd64.deb"
CB_COLUMNAR_DEB = "couchbase-columnar-enterprise_1.1.1-1234-linux_amd64.deb"
EA_DEB = "enterprise-analytics_2.2.0-1234-linux_amd64.deb"
OI_DEB = "operational-insights_3.0.0-1300-linux_amd64.deb"


def _cb_product(package_name: str) -> CBProduct:
    return CouchbaseInstaller.cb_product.fget(SimpleNamespace(package_name=package_name))


def _debuginfo_url(package_name: str, version: str, edition: str) -> str:
    return CouchbaseInstaller.debuginfo_url.func(
        SimpleNamespace(
            cb_product=_cb_product(package_name),
            options=namedtuple("options", ["edition"])(
                edition,
            ),
            url=package_name,
            build_tuple=create_build_tuple(version),
            package_name=package_name,
        )
    )


class CbProductTest(TestCase):
    """Never construct a `CouchbaseInstaller`: `__init__` opens an SSH-backed `RemoteHelper`.

    `cb_product` reads only `self.package_name`, so the descriptor is called directly against a
    stand-in object instead.
    """

    def test_couchbase_server_enterprise(self):
        self.assertEqual(_cb_product(CB_ENTERPRISE_DEB), CBProduct.COUCHBASE_SERVER)

    def test_couchbase_server_community(self):
        self.assertEqual(_cb_product(CB_COMMUNITY_DEB), CBProduct.COUCHBASE_SERVER)

    def test_enterprise_analytics(self):
        self.assertEqual(_cb_product(EA_DEB), CBProduct.ENTERPRISE_ANALYTICS)

    def test_operational_insights(self):
        self.assertEqual(_cb_product(OI_DEB), CBProduct.OPERATIONAL_INSIGHTS)

    def test_legacy_couchbase_columnar_falls_back_to_couchbase_server(self):
        """Legacy `couchbase-columnar-*` packages match no CBProduct member.

        They installed to /opt/couchbase under the `couchbase-server` systemd unit, so falling
        back to COUCHBASE_SERVER is correct, not a regression.
        """
        self.assertEqual(_cb_product(CB_COLUMNAR_DEB), CBProduct.COUCHBASE_SERVER)


class DebugPackageTest(TestCase):
    def test_couchbase_server_enterprise_deb(self):
        self.assertEqual(
            _debuginfo_url(CB_ENTERPRISE_DEB, "8.0.0-1234", "enterprise"),
            "couchbase-server-enterprise-dbgsym_8.0.0-1234-linux_amd64.deb",
        )

    def test_couchbase_server_enterprise_release_deb(self):
        self.assertEqual(
            _debuginfo_url(CB_ENTERPRISE_RELEASE_DEB, "8.0.0", "enterprise"),
            "couchbase-server-enterprise-dbgsym_8.0.0-linux_amd64.deb",
        )

    def test_couchbase_server_enterprise_rpm(self):
        self.assertEqual(
            _debuginfo_url(CB_ENTERPRISE_RPM, "8.0.0-1234", "enterprise"),
            "couchbase-server-enterprise-debuginfo-8.0.0-1234-linux.x86_64.rpm",
        )

    def test_couchbase_server_community_deb(self):
        self.assertEqual(
            _debuginfo_url(CB_COMMUNITY_DEB, "7.6.0-1234", "community"),
            "couchbase-server-community-dbg_7.6.0-1234-linux_amd64.deb",
        )

    def test_enterprise_analytics_deb(self):
        self.assertEqual(
            _debuginfo_url(EA_DEB, "2.2.0-1234", "enterprise"),
            "enterprise-analytics-dbgsym_2.2.0-1234-linux_amd64.deb",
        )

    def test_operational_insights_deb(self):
        self.assertEqual(
            _debuginfo_url(OI_DEB, "3.0.0-1300", "enterprise"),
            "operational-insights-dbgsym_3.0.0-1300-linux_amd64.deb",
        )


class _OIInstaller(CouchbaseInstaller):
    cb_product = CBProduct.OPERATIONAL_INSIGHTS

    def __init__(self, udf_executor: str, os_arch: str = "arm"):
        self.options = Namespace(
            couchbase_version="3.0.0-1092", edition="enterprise", udf_executor=udf_executor
        )
        self.cluster_spec = SimpleNamespace(
            columnar_infrastructure=True, infrastructure_settings={"os_arch": os_arch}
        )
        self.remote = SimpleNamespace(package_type="deb", distro="ubuntu", distro_version="20.04")


class UDFExecutorPackageTest(TestCase):
    BUILD_DIR = (
        "http://latestbuilds.service.couchbase.com/builds/latestbuilds/operational-insights/helios/1092"
    )

    def test_package_in_build_dir_or_voltron(self):
        name = "operational-insights-udf_3.0.0-1092-linux_arm64.deb"
        for location in (f"{self.BUILD_DIR}/{name}", f"{self.BUILD_DIR}/voltron/{name}"):
            with mock.patch.object(install, "url_exist", lambda u, loc=location: u == loc):
                self.assertEqual(_OIInstaller("").udf_executor_url, location)

    def test_package_arch_follows_spec(self):
        with mock.patch.object(install, "url_exist", lambda u: "amd64" in u):
            url = _OIInstaller("", os_arch="x86_64").udf_executor_url
        self.assertTrue(url.endswith("_amd64.deb"))

    def test_image_from_registry(self):
        self.assertIsNone(_OIInstaller("").udf_executor_image)
        self.assertEqual(
            _OIInstaller("ghcr.io/cb-vanilla/").udf_executor_image,
            "ghcr.io/cb-vanilla/operational-insights-udf:3.0.0-1092",
        )
