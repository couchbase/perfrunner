import os
from unittest import TestCase

from perfrunner.remote import api

# perfrunner.helpers.worker configures celery when it is imported, and refuses to load unless it
# knows which kind of worker it is configuring. Declare one before importing it. Importing only
# updates celery's configuration; it does not connect to a broker or start anything.
os.environ.setdefault("WORKER_TYPE", "local")

from cbagent import registry as cbagent_registry


class CollectorToolDependencyTest(TestCase):
    """Cover what happens to tool-dependent collectors when ./opt was never extracted.

    The registry is the only choke point both `get_active_collectors` and
    `get_active_prometheus_collectors` share, and unlike `create_instances` it cannot be
    bypassed by a subclass override (`system.py` and `xdcr_lag.py` already override that).
    """

    class _Collector:
        REQUIRES_CB_TOOLS = False
        PROMETHEUS_CUSTOM = False
        RECONSTRUCT_ONLY = False
        ACTIVE = True

        @classmethod
        def should_collect(cls, test, collector_flags):
            return cls.ACTIVE

        @classmethod
        def create_instances(cls, test, cluster_map):
            return [cls()]

    def setUp(self):
        self.tools_available = False
        real_available = cbagent_registry.cb_tools_available
        cbagent_registry.cb_tools_available = lambda: self.tools_available
        self.addCleanup(setattr, cbagent_registry, "cb_tools_available", real_available)

        # cbagent/collectors/libstats/remotestats.py sets env.shell at import time, so
        # importing the collectors below would leak '-o pipefail' into every later test.
        self.addCleanup(setattr, api.env, "shell", api.env.shell)

        # _active_collectors imports cbagent.collectors, and RegistryMeta registers every
        # class it defines into whatever dict is installed. Import it now, while the real
        # registry is still in place, so the swap below cannot be undone underneath us.
        import cbagent.collectors  # noqa: F401

        self.registry = cbagent_registry.CollectorRegistry()
        real_registry = self.registry._registry
        self.addCleanup(setattr, type(self.registry), "_registry", real_registry)

    def _activate(self, *collectors):
        type(self.registry)._registry = {str(i): c for i, c in enumerate(collectors)}
        return self.registry._active_collectors(test=None, cluster_map={}, merged_flags={})

    def _collector(self, **attrs):
        return type("Fake", (self._Collector,), attrs)

    def test_collectors_needing_the_tools_are_dropped_when_they_are_missing(self):
        """Creating them would buy one "command not found" per sample, for the whole run."""
        instances = self._activate(
            self._collector(REQUIRES_CB_TOOLS=True),
            self._collector(),
            self._collector(REQUIRES_CB_TOOLS=True),
        )

        self.assertEqual([type(i).REQUIRES_CB_TOOLS for i in instances], [False])

    def test_collectors_needing_the_tools_are_kept_when_they_are_there(self):
        self.tools_available = True

        instances = self._activate(
            self._collector(REQUIRES_CB_TOOLS=True), self._collector()
        )

        self.assertEqual(len(instances), 2)

    def test_a_collector_that_does_not_need_the_tools_is_unaffected(self):
        self.assertEqual(len(self._activate(self._collector(), self._collector())), 2)

    def test_an_inactive_collector_is_filtered_out_first(self):
        self.assertEqual(
            self._activate(self._collector(REQUIRES_CB_TOOLS=True, ACTIVE=False)), []
        )

    def test_the_collectors_that_run_a_tool_declare_it(self):
        """Pins the flag to the collectors whose sample loop shells out to ./opt."""
        from cbagent.collectors.cbstats import CBStatsAll, CBStatsMemory
        from cbagent.collectors.kvstore_stats import KVStoreStats, McstatHistogramStats

        for collector in (CBStatsMemory, CBStatsAll, KVStoreStats, McstatHistogramStats):
            self.assertTrue(collector.REQUIRES_CB_TOOLS, collector.__name__)
