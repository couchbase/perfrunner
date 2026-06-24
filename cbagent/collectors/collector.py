import socket
import sys
import time
import traceback
from threading import Thread
from typing import Optional, Union

import requests

from cbagent.metadata_client import MetadataClient
from cbagent.registry import RegistryMeta
from cbagent.settings import CbAgentSettings
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.server import ServerInfoManager
from perfrunner.tests import PerfTest


class Collector(metaclass=RegistryMeta):
    """General collector base: self-registration, activation and lifecycle.

    Holds everything independent of how stats are sourced, the registry
    declarations, environment guards, instance creation, metric metadata and
    the sample loop. Couchbase-specific access (REST, node/bucket discovery)
    lives in :class:`CouchbaseCollector`.
    """

    # Base class itself is not a concrete collector; registry skips it.
    ABSTRACT = True

    COLLECTOR = None

    # Flag name (from test.COLLECTORS) that activates this collector.
    # If the flag is truthy, the collector is activated.
    COLLECTOR_FLAG: Optional[str] = None

    # If True, the collector is always instantiated (no flag check needed).
    ALWAYS_ON: bool = False

    # Environment guards — set to True to restrict where the collector runs.
    SKIP_ON_DYNAMIC: bool = False  # Skip when the cluster is on K8S
    REQUIRES_ON_PREM: bool = False  # Skip when the cluster is on Capella
    REQUIRES_NON_CYGWIN: bool = False  # Skip when the platform is cygwin
    REQUIRES_NON_CLOUD: bool = False  # Skip when the cluster is on any cloud (including Capella)
    REQUIRES_CYGWIN: bool = False  # Skip when platform is NOT cygwin

    @classmethod
    def should_collect(cls, test, collector_flags: dict) -> bool:
        """Return True if this collector should be active for the given test.

        Always applies environment guards first, then delegates to
        ``is_activated`` for the config/flag decision. Subclasses with
        custom activation logic should override ``is_activated``, never
        this method - overriding here bypasses env guards.
        """
        if not cls._passes_env_guards(test):
            return False
        return cls.is_activated(test, collector_flags)

    @classmethod
    def _passes_env_guards(cls, test: PerfTest) -> bool:
        """Return True if the current test environment satisfies all guards."""
        if cls.SKIP_ON_DYNAMIC and test.dynamic_infra:
            return False
        if cls.REQUIRES_ON_PREM and test.capella_infra:
            return False
        remote = getattr(test, "remote", None)
        if cls.REQUIRES_NON_CYGWIN and remote and remote.PLATFORM == "cygwin":
            return False
        if cls.REQUIRES_CYGWIN and remote and remote.PLATFORM != "cygwin":
            return False
        if cls.REQUIRES_NON_CLOUD and test.cloud_infra:
            return False
        return True

    @classmethod
    def is_activated(cls, test, collector_flags: dict) -> bool:
        """Return True if config/flags activate this collector.

        Default implementation honours ``ALWAYS_ON`` and ``COLLECTOR_FLAG``.
        Override in subclasses with config-driven activation (e.g. enabled
        only when a specific test-config setting is set).
        """
        if cls.ALWAYS_ON:
            return True
        return bool(cls.COLLECTOR_FLAG and collector_flags.get(cls.COLLECTOR_FLAG))

    @classmethod
    def create_instances(cls, test: PerfTest, cluster_map: dict) -> list:
        """Create one collector instance per cluster.

        Every collector is constructed as ``cls(settings, test)``; collectors
        that need more than ``settings`` read it from ``test`` in their own
        ``__init__``. Override this only to change *which* instances are created
        (e.g. IO partitions, XDCR source-only).
        """
        instances = []
        for cluster_id, master_node in cluster_map.items():
            settings = CbAgentSettings(test)
            settings.cluster = cluster_id
            settings.master_node = master_node
            if settings.cloud.get("enabled") and not settings.cloud.get("dynamic", False):
                settings.hostnames = test.cluster_spec.cluster_servers(master_node)
            instances.append(cls(settings, test))
        return instances

    def __init__(self, settings: CbAgentSettings, test: Optional[PerfTest] = None):
        self._settings = settings
        self.interval = settings.interval
        self.metrics = set()
        self.updater = None

    # ``store`` and ``mc`` are lazy so an owner can swap ``store`` (e.g. for a
    # different backend) before first use, without eager-constructing a
    # PerfStore/MetadataClient that would just be discarded.
    @property
    def store(self):
        if not hasattr(self, "_store"):
            self._store = PerfStore(self._settings.cbmonitor_host)
        return self._store

    @store.setter
    def store(self, value):
        self._store = value

    @property
    def mc(self):
        if not hasattr(self, "_mc"):
            self._mc = MetadataClient(self._settings)
        return self._mc

    @mc.setter
    def mc(self, value):
        self._mc = value

    def _update_metric_metadata(self, metrics, bucket=None, index=None, server=None):
        for metric in metrics:
            metric = metric.replace("/", "_")
            metric_hash = hash((metric, bucket, index, server))
            if metric_hash not in self.metrics:
                self.metrics.add(metric_hash)
                self.mc.add_metric(metric, bucket, index, server, self.COLLECTOR)

    def update_metric_metadata(self, *args, **kwargs):
        if self.updater is None or not self.updater.is_alive():
            self.updater = Thread(target=self._update_metric_metadata, args=args, kwargs=kwargs)
            self.updater.daemon = True
            self.updater.start()
            self.updater.join()

    def sample(self):
        raise NotImplementedError

    def _init_pool(self):
        # Due to the underlying architecture of the 4.x SDK, a cluster instance cannot be shared
        # across processes. This method gives a place for collectors that use the SDK to initialise
        # the connection when the collector process is started.
        # https://docs.couchbase.com/sdk-api/couchbase-python-client/couchbase_api/parallelism.html
        pass

    def collect(self):
        self._init_pool()
        while True:
            try:
                t0 = time.time()
                self.sample()
                delta = time.time() - t0
                if delta >= self.interval:
                    continue
                time.sleep(self.interval - delta)
            except KeyboardInterrupt:
                sys.exit()
            except Exception:
                # Log the full traceback so collector failures aren't silent. Sleep
                # one interval to avoid hot-looping when the error is persistent.
                logger.warning(
                    f"Unexpected exception in {self.__class__.__name__}:\n{traceback.format_exc()}"
                )
                time.sleep(self.interval)


class CouchbaseCollector(Collector):
    """Collector specialised for talking to a Couchbase cluster over REST.

    Adds cluster connection state (master node, auth, buckets, node discovery)
    and the HTTP helpers used by concrete Couchbase collectors.
    """

    ABSTRACT = True

    def __init__(self, settings: CbAgentSettings, test: Optional[PerfTest] = None):
        super().__init__(settings, test)
        self.session = requests.Session()
        self.cloud = settings.cloud
        self.cloud_enabled = self.cloud["enabled"]
        if self.cloud_enabled:
            self.session = self.cloud["cloud_rest"]
        self.cluster = settings.cluster
        self.master_node = settings.master_node
        self.server_info = ServerInfoManager().get_server_info_by_master_node(self.master_node)
        self.is_columnar = self.server_info.is_columnar
        self.capella_infra = settings.capella_infra
        self.auth = (settings.rest_username, settings.rest_password)
        self.buckets = settings.buckets
        self.indexes = settings.indexes
        self.collections = settings.collections
        self.hostnames = settings.hostnames
        self.workers = settings.workers
        self.n2n_enabled = settings.is_n2n
        # Initialise before get_nodes(): if that first request fails it falls back to
        # refresh_nodes_and_retry, which iterates self.nodes so the attribute must
        # already exist (empty => a clear RuntimeError instead of AttributeError).
        self.nodes = []
        self.nodes = list(self.get_nodes())
        self.ssh_username = getattr(settings, "ssh_username", None)
        self.ssh_password = getattr(settings, "ssh_password", None)
        self.remote_workers = settings.remote
        if self.remote_workers:
            self.remote_worker_home = settings.remote_worker_home

    def _get_url(self, server: str, port: str, path: str) -> str:
        scheme = "http"
        if self.n2n_enabled:
            port = int(f"1{port}")
            scheme = "https"

        if self.cloud.get("dynamic", False):
            server, port = self.session.translate_host_and_port(server, port)
        return f"{scheme}://{server}:{port}{path}"

    def _request_http(
        self,
        method: str,
        path: str,
        server: Optional[str] = None,
        port: int = 8091,
        json_response: bool = True,
        json_data: Optional[dict] = None,
    ) -> Union[dict, str]:
        server = server or self.master_node

        try:
            url = self._get_url(server, port, path)
            params = {"url": url}

            if json_data is not None:
                params["json"] = json_data

            if not self.cloud_enabled:
                # When we are on cloud, self.session is a RestHelper so we shouldn't add auth
                # because it will do it for us. When not on cloud, we need it.
                params.update({"auth": self.auth, "verify": False})

            r = getattr(self.session, method.lower())(**params)

            if r.status_code in (200, 201, 202):
                return r.json() if json_response else r.text
            else:
                logger.warning(f"Bad response ({method}): {url}")
                if json_data is not None:
                    logger.warning(f"Request payload: {json_data}")
                logger.warning(f"Response text: {r.text}")
        except requests.ConnectionError:
            logger.warning(f"Connection error: {url}")

        return self.refresh_nodes_and_retry(path, server, port, json_response)

    def get_http(
        self, path: str, server: Optional[str] = None, port: int = 8091, json: bool = True
    ) -> Union[dict, str]:
        return self._request_http("GET", path, server, port, json_response=json)

    def post_http(
        self,
        path: str,
        server: Optional[str] = None,
        port: int = 8091,
        json_out: bool = True,
        json_data: Optional[dict] = None,
    ) -> Union[dict, str]:
        return self._request_http(
            "POST",
            path,
            server,
            port,
            json_response=json_out,
            json_data=json_data,
        )

    def refresh_nodes_and_retry(self, path, server=None, port=8091, json=True):
        time.sleep(self.interval)

        for node in self.nodes:
            if self._check_node(node):
                self.master_node = node
                self.nodes = list(self.get_nodes())
                break
        else:
            raise RuntimeError("Failed to find at least one node")

        if server not in self.nodes:
            raise RuntimeError(f"Bad node {server or ''}")

        return self.get_http(path, server, port, json)

    def _check_node(self, node):
        try:
            s = socket.socket()
            s.connect((node, 8091))
        except socket.error:
            return False
        else:
            if not self.get_http(path="/pools", server=node).get("pools"):
                return False
        return True

    def get_buckets(self, with_stats=False):
        if self.is_columnar:
            # Columnar clusters don't have KV buckets so don't try to get them.
            return

        buckets = self.get_http(path="/pools/default/buckets")
        if not buckets:
            buckets = self.refresh_nodes_and_retry(path="/pools/default/buckets")
        for bucket in buckets:
            if self.buckets is not None and bucket["name"] not in self.buckets:
                continue
            if with_stats:
                yield bucket["name"], bucket["stats"]
            else:
                yield bucket["name"]

    def get_nodes(self):
        if self.cloud_enabled and not self.cloud["dynamic"]:
            # Can't get nodes from /pools/default because depending on the cloud provider,
            # pools/default will return private IPs instead of public IPs or DNS names
            for hostname in self.hostnames:
                yield hostname
        else:
            pool = self.get_http(path="/pools/default")
            for node in pool["nodes"]:
                hostname = node["hostname"].split(":")[0]
                if self.hostnames is not None and hostname not in self.hostnames:
                    continue
                yield hostname

    def get_all_indexes(self):
        if self.collections:
            scopes = self.indexes[self.buckets[0]]
            for scope_name, collections in scopes.items():
                for collection_name, index_defs in collections.items():
                    for index in index_defs:
                        yield index, self.buckets[0], scope_name, collection_name
        else:
            for index in self.indexes:
                yield index, self.buckets[0], None, None
