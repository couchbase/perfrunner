from collections import OrderedDict

from perfrunner.tests import PerfTest


class RegistryMeta(type):
    """Metaclass that auto-registers every Collector subclass at definition time.

    When a class using this metaclass is created, it is automatically added to
    the global CollectorRegistry singleton. Classes with ``ABSTRACT = True``
    (set on the class itself, not inherited) are filtered out by ``register``.
    """

    def __new__(mcs, name, bases, attrs):
        new_cls = super().__new__(mcs, name, bases, attrs)
        CollectorRegistry().register(new_cls)
        return new_cls


class CollectorRegistry:
    """Singleton registry of all collector classes.

    Populated automatically by ``RegistryMeta`` at import time.
    """

    _instance = None
    _registry = {}
    # Default flags that will by default be registered. Can be overridden by test-specific flags.
    DEFAULT_COLLECTOR_FLAGS = {
        "active_tasks": True,
        "iostat": True,
        "memory": True,
        "net": True,
        "ns_server": True,
    }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CollectorRegistry, cls).__new__(cls)
        return cls._instance

    def register(self, cls):
        if not cls.__dict__.get("ABSTRACT", False):
            self._registry[cls.__name__] = cls

    def _merge_flags(self, test: PerfTest, explicit_flags: dict) -> dict:
        """Combine default, test-specified and force-enabled collector flags.

        Precedence (low -> high): DEFAULT_COLLECTOR_FLAGS, the test's explicit
        flags, then ``stats.extra_collectors`` — a test-config escape hatch that
        force-activates named collectors regardless of what the test specified.
        """
        merged = {**self.DEFAULT_COLLECTOR_FLAGS, **explicit_flags}
        for flag in getattr(test.stats_settings, "extra_collectors", []):
            merged[flag] = True
        return merged

    def _active_collectors(
        self, test: PerfTest, cluster_map: dict, merged_flags: dict, prometheus_only: bool = False
    ):
        """Instantiate every registered collector that should be active for the test.

        ``prometheus_only`` additionally restricts to collectors that push custom metrics.
        One instance is created per cluster in ``cluster_map`` via ``create_instances``.
        """
        # Import so every collector class is registered (via RegistryMeta) before we
        # iterate ``_registry``. Relies on ``cbagent/collectors/__init__.py`` importing
        # every collector module.
        import cbagent.collectors  # noqa: F401

        collectors = []
        for cls in self._registry.values():
            if prometheus_only and not getattr(cls, "PROMETHEUS_CUSTOM", False):
                continue
            if cls.should_collect(test, merged_flags):
                collectors.extend(cls.create_instances(test, cluster_map))
        return collectors

    def get_active_collectors(self, test: PerfTest, cluster_map: dict, **collector_flags):
        """Return instances of all collectors that should be active for the test."""
        return self._active_collectors(test, cluster_map, self._merge_flags(test, collector_flags))

    def get_active_prometheus_collectors(self, test: PerfTest):
        """Return active collectors that push custom metrics.

        The Prometheus path has no CbAgent to build a cluster map, so derive it here
        (cluster id -> master node) from the test's snapshot clusters.
        """
        cluster_map = OrderedDict(zip(test.cbmonitor_clusters, test.cluster_spec.masters))
        return self._active_collectors(
            test, cluster_map, self._merge_flags(test, test.COLLECTORS), prometheus_only=True
        )
