from typing import List

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task
from perfrunner.remote.api import CommandTimeout


class SysdigStat(RemoteStats):

    SAMPLING_INTERVAL = 2  # seconds

    SYSTEM_CALLS = 'pread', 'pwrite'

    def get_call_rate(self, process: str, syscall: str) -> float:
        evt_filter = f"proc.pid=`pgrep {process}` and evt.type={syscall} and evt.dir=>"
        cmd = f'sysdig -M{self.SAMPLING_INTERVAL} -p "%evt.num" "{evt_filter}" | wc -l'

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
                key = f"{process}_{syscall}"
                samples[key] = self.get_call_rate(process, syscall)
        return samples
