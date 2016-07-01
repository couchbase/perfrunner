from cbagent.collectors.libstats.atopstats import AtopStats
from cbagent.collectors import Collector


class Atop(Collector):

    COLLECTOR = "atop"

    METRICS = ("beam.smp_rss", "memcached_rss", "indexer_rss", "projector_rss",
               "cbq-engine_rss", "beam.smp_vsize", "memcached_vsize",
               "indexer_vsize", "projector_vsize", "beam.smp_cpu",
               "memcached_cpu", "indexer_cpu", "projector_cpu", "cbq-engine_cpu",
               "goxdcr_cpu", "goxdcr_rss", "cbft_vsize", "cbft_rss", "cbft_cpu",)

    def __init__(self, settings):
        super(Atop, self).__init__(settings)
        self.atop = AtopStats(hosts=tuple(self.get_nodes()),
                              user=self.ssh_username,
                              password=self.ssh_password)

    def restart(self):
        self.atop.restart_atop()

    def update_columns(self):
        self.atop.update_columns()

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.get_nodes():
            self.mc.add_server(node)
            for metric in self.METRICS:
                self.mc.add_metric(metric, server=node,
                                   collector=self.COLLECTOR)

    @staticmethod
    def _remove_value_units(value):
        if value is None:
            return
        for magnitude, denotement in enumerate(("K", "M", "G"), start=1):
            if denotement in value:
                return float(value.replace(denotement, "")) * 1024 ** magnitude
        if "%" in value:
            return float(value.replace("%", ""))
        else:
            return float(value)

    def _format_data(self, data):
        sample = dict()
        for node, (title, value) in data.iteritems():
            sample[node] = sample.get(node, dict())
            sample[node][title] = self._remove_value_units(value)
        return sample

    def _extend_samples(self, data):
        data = self._format_data(data)
        if not self._samples:
            self._samples = data
        else:
            for node in self._samples:
                self._samples[node].update(data[node])

    def sample(self):
        self._samples = {}
        self._extend_samples(self.atop.get_process_rss("beam.smp"))
        self._extend_samples(self.atop.get_process_vsize("beam.smp"))
        self._extend_samples(self.atop.get_process_rss("memcached"))
        self._extend_samples(self.atop.get_process_vsize("memcached"))
        self._extend_samples(self.atop.get_process_cpu("beam.smp"))
        self._extend_samples(self.atop.get_process_cpu("memcached"))

        for node, samples in self._samples.iteritems():
            self.store.append(samples, cluster=self.cluster, server=node,
                              collector=self.COLLECTOR)
