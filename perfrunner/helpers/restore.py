from collections import defaultdict

from fabric.api import run, settings
from logger import logger

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.utils.cluster import ClusterManager


class RestoreHelper(object):

    def __init__(self, cluster_spec, test_config, verbose):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.verbose = verbose

    def fetch_maps(self):
        rest = RestHelper(self.cluster_spec)
        master_node = self.cluster_spec.yield_masters().next()

        maps = {}
        for bucket in self.test_config.buckets:
            vbmap = rest.get_vbmap(master_node, bucket)
            server_list = rest.get_server_list(master_node, bucket)
            maps[bucket] = (vbmap, server_list)

        return maps

    def restore(self):
        snapshot = self.test_config.restore_settings.snapshot

        maps = self.fetch_maps()

        remote = RemoteHelper(self.cluster_spec, self.test_config,
                              self.verbose)
        remote.stop_server()

        for bucket, (vbmap, server_list) in maps.items():
            files = defaultdict(list)

            for vb_idx, nodes in enumerate(vbmap):
                for node_idx in nodes:
                    files[server_list[node_idx]].append(vb_idx)

            for server, vbuckets in files.items():
                logger.info('Restoring files on {}'.format(server))

                cmd = 'cp '
                for vbucket in vbuckets:
                    cmd += '{}/{}.couch.1 '.format(snapshot, vbucket)
                cmd += '/data/{}'.format(bucket)

                with settings(host_string=server,
                              user=self.cluster_spec.ssh_credentials[0],
                              password=self.cluster_spec.ssh_credentials[1]):
                    run(cmd)

        remote.drop_caches()
        remote.start_server()

    def warmup(self):
        cm = ClusterManager(self.cluster_spec, self.test_config, self.verbose)
        cm.wait_until_warmed_up()
        cm.wait_until_healthy()
