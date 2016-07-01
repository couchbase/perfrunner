from uuid import uuid4

from cbagent.collectors.libstats.remotestats import (
    RemoteStats, multi_node_task, single_node_task)


class AtopStats(RemoteStats):

    def __init__(self, hosts, user, password):
        super(AtopStats, self).__init__(hosts, user, password)
        self.logfile = "/tmp/{}.atop".format(uuid4().hex)

        self._base_cmd =\
            "d=`date +%H:%M` && atop -r {} -b $d -e $d".format(self.logfile)

    @multi_node_task
    def stop_atop(self):
        self.run("killall -q atop")
        self.run("rm -rf /tmp/*.atop")

    @multi_node_task
    def start_atop(self):
        self.run(
            "nohup atop -a -w {} 5 > /dev/null 2>&1 &".format(self.logfile),
            pty=False)

    @single_node_task
    def update_columns(self):
        self._cpu_column = self._get_cpu_column()
        self._vsize_column = self._get_vsize_column()
        self._rss_column = self._get_rss_column()

    def restart_atop(self):
        self.stop_atop()
        self.start_atop()

    @single_node_task
    def _get_vsize_column(self):
        output = self.run("atop -m 1 1 | grep PID")
        return output.split().index("VSIZE")

    @single_node_task
    def _get_rss_column(self):
        output = self.run("atop -m 1 1 | grep PID")
        return output.split().index("RSIZE")

    @single_node_task
    def _get_cpu_column(self):
        output = self.run("atop 1 1 | grep PID")
        return output.split().index("CPU")

    def _get_metric(self, cmd, column):
        result = self.run(cmd)
        if not result.return_code:
            return result.split()[column]
        else:
            return None

    @multi_node_task
    def get_process_cpu(self, process):
        title = process + "_cpu"
        cmd = "{} | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._cpu_column)

    @multi_node_task
    def get_process_vsize(self, process):
        title = process + "_vsize"
        cmd = "{} -m | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._vsize_column)

    @multi_node_task
    def get_process_rss(self, process):
        title = process + "_rss"
        cmd = "{} -m | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._rss_column)
