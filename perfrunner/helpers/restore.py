import time
from collections import defaultdict
from threading import Thread

from fabric import state
from fabric.api import run, settings
from logger import logger

from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper


class RestoreHelper:

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.snapshot = self.test_config.restore_settings.snapshot

        self.remote = RemoteHelper(self.cluster_spec, self.test_config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def fetch_maps(self):
        rest = RestHelper(self.cluster_spec)
        master_node = next(self.cluster_spec.yield_masters())

        maps = {}
        for bucket in self.test_config.buckets:
            vbmap = rest.get_vbmap(master_node, bucket)
            server_list = rest.get_server_list(master_node, bucket)
            maps[bucket] = (vbmap, server_list)

        return maps

    def cp(self, server, cmd):
        logger.info('Restoring files on {}'.format(server))

        with settings(host_string=server,
                      user=self.cluster_spec.ssh_credentials[0],
                      password=self.cluster_spec.ssh_credentials[1]):
            run(cmd)

    def restore(self):
        maps = self.fetch_maps()

        self.remote.stop_server()

        threads = []

        for bucket, (vbmap, server_list) in maps.items():
            files = defaultdict(list)

            for vb_idx, nodes in enumerate(vbmap):
                for node_idx in nodes:
                    files[server_list[node_idx]].append(vb_idx)

            for server, vbuckets in files.items():
                cmd = 'cp '
                for vbucket in vbuckets:
                    cmd += '{}/{}.couch.1 '.format(self.snapshot, vbucket)
                cmd += '/data/{}'.format(bucket)

                threads.append(Thread(target=self.cp, args=(server, cmd)))

        for t in threads:
            t.start()
            time.sleep(1)
        for t in threads:
            t.join()
        state.connections.clear()

        self.remote.drop_caches()
        self.remote.start_server()

    def warmup(self):
        cm = ClusterManager(self.cluster_spec, self.test_config)
        cm.wait_until_warmed_up()
        cm.wait_until_healthy()
