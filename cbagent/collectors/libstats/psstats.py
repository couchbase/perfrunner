from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class PSStats(RemoteStats):

    METRICS = (
        ("rss", 1024),    # kB -> B
        ("vsize", 1024),
    )

    PS_CMD = "ps -eo pid,rss,vsize,comm | " \
        "grep {} | grep -v grep | sort -n -k 2 | tail -n 1"

    TOP_CMD = "top -b -n2 -d{0} -p {1} | grep '^\\s*{1}' | tail -n 1"

    MAX_TOP_INTERVAL = 10  # seconds

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.top_interval = min(max(1, self.interval - 1), self.MAX_TOP_INTERVAL)

    @parallel_task(server_side=True)
    def get_server_samples(self, process):
        return self.get_samples(process)

    @parallel_task(server_side=False)
    def get_client_samples(self, process):
        return self.get_samples(process)

    def get_samples(self, process):
        samples = {}

        stdout = self.run(self.PS_CMD.format(process), quiet=True)
        if stdout:
            for i, value in enumerate(stdout.split()[1:1 + len(self.METRICS)]):
                metric, multiplier = self.METRICS[i]
                title = "{}_{}".format(process, metric)
                samples[title] = float(value) * multiplier
            pid = stdout.split()[0]
        else:
            return samples

        stdout = self.run(self.TOP_CMD.format(self.top_interval, pid),
                          quiet=True)
        if stdout:
            title = "{}_cpu".format(process)
            samples[title] = float(stdout.split()[8])
        return samples
