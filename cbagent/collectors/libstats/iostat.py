from cbagent.collectors.libstats.remotestats import (
    RemoteStats, multi_node_task)


class IOstat(RemoteStats):

    METRICS = (
        ("rps", "r/s", 1),
        ("wps", "w/s", 1),
        ("rbps", "rkB/s", 1024),  # kB -> B
        ("wbps", "wkB/s", 1024),  # kB -> B
        ("avgqusz", "avgqu-sz", 1),
        ("await", "await", 1),
        ("util", "%util", 1),
    )

    def get_device_name(self, partition):
        for path in (partition, '/'):
            stdout = self.run("df '{}'| head -2 | tail -1".format(path),
                              warn_only=True, quiet=True)
            if not stdout.return_code:
                name = stdout.split()[0]
                if name.startswith('/dev/mapper/'):
                    return name.split('/dev/mapper/')[1]
                else:
                    return name

    def get_iostat(self, device):
        stdout = self.run(
            "iostat -xk 1 2 -N {} | grep -v '^$' | tail -n 2".format(device)
        )
        stdout = stdout.split()
        header = stdout[:len(stdout) / 2]
        data = dict()
        for i, value in enumerate(stdout[len(stdout) / 2:]):
            data[header[i]] = value
        return data

    @multi_node_task
    def get_samples(self, partitions):
        samples = {}

        for purpose, partition in partitions.items():
            device = self.get_device_name(partition)
            data = self.get_iostat(device)
            for shorthand, metric, multiplier in self.METRICS:
                key = "{}_{}".format(purpose, shorthand)
                samples[key] = float(data[metric]) * multiplier
        return samples
