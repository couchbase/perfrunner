from typing import Dict, Optional, Tuple

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class IOStat(RemoteStats):

    METRICS = (
        ("rps", "r/s", 1),
        ("wps", "w/s", 1),
        ("rbps", "rkB/s", 1024),  # kB -> B
        ("wbps", "wkB/s", 1024),  # kB -> B
        ("avgqusz", "aqu-sz", 1),
        ("util", "%util", 1),
    )

    def get_device_name(self, path: str) -> Tuple[Optional[str], bool]:
        stdout = self.run(f"df '{path}' | head -2 | tail -1", quiet=True)
        if not stdout.return_code:
            name = stdout.split()[0]
            if name.startswith('/dev/mapper/') or name.startswith('/dev/md'):
                # LVM devices are named /dev/mapper/<vg>-<lv>
                # Software RAID devices are named /dev/md<id>
                return name, True
            else:
                return name, False
        return None, None

    def get_iostat(self, device: str) -> Dict[str, str]:
        stdout = self.run(f"iostat -dkxyN 1 1 {device} | grep -v '^$' | tail -n 2")
        stdout = stdout.split()
        header = stdout[:len(stdout) // 2]
        data = dict()
        for i, value in enumerate(stdout[len(stdout) // 2:]):
            data[header[i]] = value
        return data

    @parallel_task(server_side=True)
    def get_server_samples(self, partitions: dict) -> dict:
        return self.get_samples(partitions['server'], self.METRICS)

    @parallel_task(server_side=False)
    def get_client_samples(self, partitions: dict) -> dict:
        return self.get_samples(partitions['client'], self.METRICS)

    def get_samples(self, partitions: Dict[str, str],
                    metrics: Tuple[Tuple[str, str, int]]) -> Dict[str, float]:
        samples = {}

        for purpose, path in partitions.items():
            device, _ = self.get_device_name(path)
            if device is not None:
                stats = self.get_iostat(device)
                for metric, column, multiplier in metrics:
                    key = f"{purpose}_{metric}"
                    samples[key] = float(stats[column]) * multiplier

        return samples


class DiskStats(IOStat):

    def get_disk_stats(self, device: str):
        device_name = device.split('/')[-1]

        # https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
        stdout = self.run(f"grep '{device_name}' /proc/diskstats")
        stats = stdout.split()
        sectors_read, sectors_written = int(stats[5]), int(stats[9])

        # https://www.kernel.org/doc/Documentation/block/queue-sysfs.txt
        if 'nvme' in device and 'p1' not in device and 'p2' not in device:
            stdout = self.run(f"cat /sys/block/{device_name}/queue/hw_sector_size")
        else:
            parent = self.run(f"lsblk -no pkname {device}").strip()
            stdout = self.run(f"cat /sys/block/{parent}/queue/hw_sector_size")
        sector_size = int(stdout)

        return sectors_read * sector_size, sectors_written * sector_size

    @parallel_task(server_side=True)
    def get_server_samples(self, partitions: dict) -> dict:
        return self.get_samples(partitions['server'])

    def get_samples(self, partitions: dict) -> dict:
        samples = {}
        for purpose, partition in partitions.items():
            device, lvm_swraid = self.get_device_name(partition)
            if device is not None and not lvm_swraid:
                bytes_read, bytes_written = self.get_disk_stats(device)
                samples[purpose + '_bytes_read'] = bytes_read
                samples[purpose + '_bytes_written'] = bytes_written
        return samples
