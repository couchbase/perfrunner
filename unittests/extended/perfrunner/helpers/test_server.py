"""`_initialise_server_info`'s version-suffix strip chain and `is_columnar` detection.

Extended, not core: importing `perfrunner.helpers.server` drags in `perfrunner.helpers.rest`,
which the core tier's purity guard forbids (network-capable).
"""

from types import SimpleNamespace
from unittest import TestCase

from perfrunner.helpers.server import ServerInfoManager


class InitialiseServerInfoTest(TestCase):
    def _server_info(self, raw_version: str):
        # `_initialise_server_info` never touches `self`, so call it unbound to sidestep the
        # `ServerInfoManager` singleton machinery in `__new__`/`_initialise`.
        rest_helper = SimpleNamespace(get_version_raw=lambda host: raw_version)
        return ServerInfoManager._initialise_server_info(None, "10.0.0.1", rest_helper)

    def test_operational_insights(self):
        """Before this fix, `create_build_tuple` raised ValueError on the underscore suffix."""
        info = self._server_info("3.0.0-1300-operational_insights")
        self.assertTrue(info.is_columnar)
        self.assertEqual(info.build_tuple, (3, 0, 0, 1300))

    def test_enterprise_analytics(self):
        info = self._server_info("2.2.0-1234-enterprise-analytics")
        self.assertTrue(info.is_columnar)
        self.assertEqual(info.build_tuple, (2, 2, 0, 1234))

    def test_plain_server_is_not_columnar(self):
        info = self._server_info("7.6.0-1234-enterprise")
        self.assertFalse(info.is_columnar)
        self.assertEqual(info.build_tuple, (7, 6, 0, 1234))
