import os

from cbagent.collectors.collector import Collector


class SecondaryLatencyStats(Collector):

    COLLECTOR = "secondaryscan_latency"

    SECONDARY_STATS_FILE = '/root/statsfile'

    def __init__(self, settings):
        super().__init__(settings)
        self.interval = settings.lat_interval

    def _get_secondaryscan_latency(self):
        stats = {}
        if os.path.isfile(self.SECONDARY_STATS_FILE):
            with open(self.SECONDARY_STATS_FILE, 'rb') as fh:
                try:
                    next(fh).decode()
                    fh.seek(-200, 2)
                    last = fh.readlines()[-1].decode()
                    duration = last.split(',')[-1]
                    stats = {}
                    latency = duration.split(':')[1]
                    latency = latency.rstrip()
                    latency_key = duration.split(':')[0]
                    latency_key = latency_key.strip()
                    stats[latency_key] = int(latency)
                except (StopIteration, IOError):
                    pass
        return stats

    def sample(self):
        stats = self._get_secondaryscan_latency()
        if stats:
            self.update_metric_metadata(stats.keys())
            self.store.append(stats, cluster=self.cluster, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
