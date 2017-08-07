from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class MemInfo(RemoteStats):

    def get_mem_stats(self) -> dict:
        stats = {}
        stdout = self.run('cat /proc/meminfo')
        for line in stdout.splitlines():
            fields = line.split()
            metric, value = fields[0], fields[1]
            metric = metric.rstrip(':')
            value = 1024 * int(value)  # KB -> Bytes
            stats[metric] = value
        return stats

    @parallel_task(server_side=True)
    def get_samples(self):
        return self.get_mem_stats()
