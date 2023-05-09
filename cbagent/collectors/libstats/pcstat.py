from typing import Tuple

import numpy

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class PCStat(RemoteStats):

    def get_pcstat(self, partition: str) -> float:
        stdout = self.run(
            'find {} -regex .*[0-9]+\\.couch\\.[0-9]+ \
            | xargs pcstat -nohdr -terse 2>/dev/null'.format(partition),
        )
        percents = []
        for line in stdout.splitlines():
            # Header: name,size,timestamp,mtime,pages,cached,percent
            percent = line.split(',')[-1]
            percents.append(float(percent))
        return numpy.average(percents)

    def get_cachestat(self) -> Tuple[float, float]:
        stdout = self.run('cachestat')
        total_hits, hit_ratio = stdout.split()
        return float(total_hits), float(hit_ratio)

    @parallel_task(server_side=True)
    def get_samples(self, partitions: dict) -> dict:
        total_hits, hit_ratio = self.get_cachestat()
        samples = {
            'page_cache_hit_ratio': hit_ratio,
            'page_cache_total_hits': total_hits,
        }

        for purpose, partition in partitions.items():
            key = "{}_avg_page_cache_rr".format(purpose)
            samples[key] = self.get_pcstat(partition)

        return samples
