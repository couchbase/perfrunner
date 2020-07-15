from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class VMStat(RemoteStats):

    def get_vmstat(self) -> dict:
        stats = {'allocstall': 0}
        stdout = self.run('cat /proc/vmstat')
        for line in stdout.splitlines():
            fields = line.split()
            metric, value = fields[0], fields[1]
            if 'allocstall' in metric:
                stats['allocstall'] += int(value)
            else:
                stats[metric] = int(value)
        return stats

    @parallel_task(server_side=True)
    def get_samples(self):
        return self.get_vmstat()
