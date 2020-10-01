from typing import Dict, Union

from cbagent.collectors.libstats.remotestats import RemoteStats, parallel_task


class IOStat(RemoteStats):

    METRICS_Centos = (
        ("rps", "r/s", 1),
        ("wps", "w/s", 1),
        ("rbps", "rkB/s", 1024),  # kB -> B
        ("wbps", "wkB/s", 1024),  # kB -> B
        ("avgqusz", "avgqu-sz", 1),
        ("await", "await", 1),
        ("util", "%util", 1),
    )

    METRICS_Ubuntu = (
        ("rps", "r/s", 1),
        ("wps", "w/s", 1),
        ("rbps", "rkB/s", 1024),  # kB -> B
        ("wbps", "wkB/s", 1024),  # kB -> B
        ("avgqusz", "aqu-sz", 1),
        ("util", "%util", 1),
    )

    def get_device_name(self, path: str) -> [Union[None, str], bool]:
        stdout = self.run("df '{}' | head -2 | tail -1".format(path),
                          quiet=True)
        if not stdout.return_code:
            name = stdout.split()[0]
            if name.startswith('/dev/mapper/'):  # LVM
                return name.split('/dev/mapper/')[1], True
            elif name.startswith('/dev/md'):  # Software RAID
                return name, True
            else:
                return name, False
        return None, None

    def get_iostat(self, device: str) -> Dict[str, str]:
        stdout = self.run(
            "iostat -dkxyN 1 1 {} | grep -v '^$' | tail -n 2".format(device)
        )
        stdout = stdout.split()
        header = stdout[:len(stdout) // 2]
        data = dict()
        for i, value in enumerate(stdout[len(stdout) // 2:]):
            data[header[i]] = value
        return data

    @parallel_task(server_side=True)
    def get_server_samples(self, partitions: dict) -> dict:
        return self.get_samples_centos(partitions['server'])

    @parallel_task(server_side=False)
    def get_client_samples(self, partitions: dict) -> dict:
        return self.get_samples_ubuntu(partitions['client'])

    def get_samples_centos(self, partitions: Dict[str, str]) -> Dict[str, float]:
        samples = {}

        for purpose, path in partitions.items():
            device, _ = self.get_device_name(path)
            if device is not None:
                stats = self.get_iostat(device)
                for metric, column, multiplier in self.METRICS_Centos:
                    key = "{}_{}".format(purpose, metric)
                    samples[key] = float(stats[column]) * multiplier

        return samples

    def get_samples_ubuntu(self, partitions: Dict[str, str]) -> Dict[str, float]:
        samples = {}

        for purpose, path in partitions.items():
            device, _ = self.get_device_name(path)
            if device is not None:
                stats = self.get_iostat(device)
                for metric, column, multiplier in self.METRICS_Ubuntu:
                    key = "{}_{}".format(purpose, metric)
                    samples[key] = float(stats[column]) * multiplier

        return samples


class DiskStats(IOStat):

    def get_disk_stats(self, device: str):
        device_name = device.split('/')[-1]

        # https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
        stdout = self.run("grep '{}' /proc/diskstats".format(device_name))
        stats = stdout.split()
        sectors_read, sectors_written = int(stats[5]), int(stats[9])

        # https://www.kernel.org/doc/Documentation/block/queue-sysfs.txt
        parent = self.run('lsblk -no pkname {}'.format(device)).strip()
        stdout = self.run('cat /sys/block/{}/queue/hw_sector_size'.format(parent))
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
