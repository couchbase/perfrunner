from collections import defaultdict

from cbagent.collectors.libstats.remotestats import (
    RemoteStats,
    multi_node_task,
)


class NetStat(RemoteStats):

    def detect_iface(self):
        """Examples of output:

            default via 172.23.100.1 dev enp5s0f0 onlink
            default via 172.23.96.1 dev enp6s0  proto static  metric 1024
            default via 172.23.100.1 dev em1  proto static
        """
        stdout = self.run("ip route list | grep default")
        return stdout.strip().split()[4]

    def get_dev_stats(self):
        iface = self.detect_iface()
        cmd = "grep {} /proc/net/dev".format(iface)
        stdout = self.run("{0}; sleep 1; {0}".format(cmd))
        s1, s2 = stdout.split('\n')
        s1 = [int(v.split(":")[-1]) for v in s1.split() if v.split(":")[-1]]
        s2 = [int(v.split(":")[-1]) for v in s2.split() if v.split(":")[-1]]
        return {
            "in_bytes_per_sec": s2[0] - s1[0],
            "out_bytes_per_sec": s2[8] - s1[8],
            "in_packets_per_sec": s2[1] - s1[1],
            "out_packets_per_sec": s2[9] - s1[9],
        }

    def get_tcp_stats(self):
        stdout = self.run("cat /proc/net/tcp")
        raw_data = defaultdict(int)
        for conn in stdout.split("\n"):
            state = conn.split()[3]
            raw_data[state] += 1
        return {
            "ESTABLISHED": raw_data["01"],
            "TIME_WAIT": raw_data["06"],
        }

    @multi_node_task
    def get_samples(self):
        dev_stats = self.get_dev_stats()
        tcp_stats = self.get_tcp_stats()
        return dict(dev_stats, **tcp_stats)
