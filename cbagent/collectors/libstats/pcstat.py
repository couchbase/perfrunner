import numpy

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class PCStat(RemoteStats):

    def get_pcstat(self, partition: str) -> float:
        stdout = self.run(
            'find {} -regex .*[0-9]+\.couch\.[0-9]+ \
            | xargs pcstat -nohdr -terse 2>/dev/null'.format(partition),
        )
        percents = []
        for line in stdout.splitlines():
            # Header: name,size,timestamp,mtime,pages,cached,percent
            percent = line.split(',')[-1]
            percents.append(float(percent))
        return numpy.average(percents)

    @parallel_task(server_side=True)
    def get_server_samples(self, partitions: dict) -> dict:
        return self.get_samples(partitions['server'])

    def get_samples(self, partitions: dict) -> dict:
        samples = {}
        for purpose, partition in partitions.items():
            key = "{}_avg_page_cache_rr".format(purpose)
            samples[key] = self.get_pcstat(partition)
        return samples
