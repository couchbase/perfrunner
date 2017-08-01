from typing import List

from fabric.exceptions import CommandTimeout

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class SysdigStat(RemoteStats):

    SAMPLING_INTERVAL = 2  # seconds

    SYSTEM_CALLS = 'pread', 'pwrite'

    def get_call_rate(self, process: str, syscall: str) -> float:
        evt_filter = 'proc.pid=`pgrep {}` and evt.type={} and evt.dir=>'\
            .format(process, syscall)
        cmd = 'sysdig -M{} -p "%evt.num" "{}" | wc -l'.format(
            self.SAMPLING_INTERVAL, evt_filter)

        try:
            stdout = self.run(cmd, timeout=5, quiet=True)
        except CommandTimeout:
            return 0
        else:
            num_calls = int(stdout)
            return num_calls / self.SAMPLING_INTERVAL

    @parallel_task(server_side=True)
    def get_samples(self, processes: List[str]) -> dict:
        samples = {}
        for process in processes:
            for syscall in self.SYSTEM_CALLS:
                key = "{}_{}".format(process, syscall)
                samples[key] = self.get_call_rate(process, syscall)
        return samples
