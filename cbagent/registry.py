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

    def get_active_collectors(self, test, cluster_map, **collector_flags):
        """Return instances of all collectors that should be active for the test."""
        # This ensures that all collector classes are registered at import time, so that
        # the CollectorRegistry singleton is populated and ready to use. Relies on
        # ``cbagent/collectors/__init__.py`` having imported every collector module
        import cbagent.collectors  # noqa: F401

        # Merge defaults with explicit flags (explicit flags take precedence).
        merged_flags = {**self.DEFAULT_COLLECTOR_FLAGS, **collector_flags}

        collectors = []
        for _, cls in self._registry.items():
            if cls.should_collect(test, merged_flags):
                instances = cls.create_instances(test, cluster_map)
                collectors.extend(instances)
        return collectors
