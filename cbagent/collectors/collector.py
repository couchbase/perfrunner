import socket
import sys
import time
from threading import Thread

import requests

from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from logger import logger


class Collector:

    COLLECTOR = None

    def __init__(self, settings):
        self.session = requests.Session()

        self.interval = settings.interval

        self.cluster = settings.cluster
        self.master_node = settings.master_node
        self.auth = (settings.rest_username, settings.rest_password)
        self.buckets = settings.buckets
        self.indexes = settings.indexes
        self.hostnames = settings.hostnames
        self.workers = settings.workers
        self.nodes = list(self.get_nodes())
        self.ssh_username = getattr(settings, 'ssh_username', None)
        self.ssh_password = getattr(settings, 'ssh_password', None)

        self.store = PerfStore(settings.cbmonitor_host)
        self.mc = MetadataClient(settings)

        self.metrics = set()
        self.updater = None

    def get_http(self, path, server=None, port=8091, json=True):
        server = server or self.master_node
        url = "http://{}:{}{}".format(server, port, path)
        try:
            r = self.session.get(url=url, auth=self.auth)
            if r.status_code in (200, 201, 202):
                return json and r.json() or r.text
            else:
                logger.warn("Bad response: {}".format(url))
                return self.refresh_nodes_and_retry(path, server, port)
        except requests.ConnectionError:
            logger.warn("Connection error: {}".format(url))
            return self.refresh_nodes_and_retry(path, server, port, json)

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
            raise RuntimeError("Bad node {}".format(server or ""))

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
        pool = self.get_http(path="/pools/default")
        for node in pool["nodes"]:
            hostname = node["hostname"].split(":")[0]
            if self.hostnames is not None and hostname not in self.hostnames:
                continue
            yield hostname

    def _update_metric_metadata(self, metrics, bucket=None, index=None, server=None):
        for metric in metrics:
            metric = metric.replace('/', '_')
            metric_hash = hash((metric, bucket, index, server))
            if metric_hash not in self.metrics:
                self.metrics.add(metric_hash)
                self.mc.add_metric(metric, bucket, index, server, self.COLLECTOR)

    def update_metric_metadata(self, *args, **kwargs):
        if self.updater is None or not self.updater.is_alive():
            self.updater = Thread(
                target=self._update_metric_metadata, args=args, kwargs=kwargs
            )
            self.updater.daemon = True
            self.updater.start()
            self.updater.join()

    def sample(self):
        raise NotImplementedError

    def collect(self):
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
            except IndexError:
                pass
            except Exception as e:
                logger.warn("Unexpected exception in {}: {}"
                            .format(self.__class__.__name__, e))
