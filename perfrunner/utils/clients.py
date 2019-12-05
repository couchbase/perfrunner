from argparse import ArgumentParser

from fabric.api import cd, local, run

from logger import logger
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import all_clients
from perfrunner.settings import ClusterSpec, TestConfig

LIBCOUCHBASE_BASE_URL = "https://github.com/couchbase/libcouchbase/releases/download"
LIBCOUCHBASE_PACKAGES = [{"version": "3.0.0-alpha.5",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.0+alpha.5_ubuntu1604_xenial_amd64",
                          "package_path": "libcouchbase-3.0.0+alpha.5_ubuntu1604_xenial_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.0+alpha.5-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.0+alpha.5-1_amd64.deb "
                               "libcouchbase-dbg_3.0.0+alpha.5-1_amd64.deb "
                               "libcouchbase3-libev_3.0.0+alpha.5-1_amd64.deb "
                               "libcouchbase3-tools_3.0.0+alpha.5-1_amd64.deb "
                               "libcouchbase-dev_3.0.0+alpha.5-1_amd64.deb"]},
                         {"version": "2.9.3",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.3_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.9.3_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.9.3-1_amd64.deb "
                              "libcouchbase2-libevent_2.9.3-1_amd64.deb "
                              "libcouchbase-dev_2.9.3-1_amd64.deb "
                              "libcouchbase2-bin_2.9.3-1_amd64.deb"]},
                         {"version": "3.0.0-beta.2",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.0_beta.2_ubuntu1604_xenial_amd64",
                          "package_path": "libcouchbase-3.0.0~beta.2_ubuntu1604_xenial_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.0~beta.2-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.0~beta.2-1_amd64.deb "
                               "libcouchbase-dbg_3.0.0~beta.2-1_amd64.deb "
                               "libcouchbase3-libev_3.0.0~beta.2-1_amd64.deb "
                               "libcouchbase3-tools_3.0.0~beta.2-1_amd64.deb "
                               "libcouchbase-dev_3.0.0~beta.2-1_amd64.deb"]},
                         {"version": "3.0.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.0_ubuntu1604_xenial_amd64",
                          "package_path": "libcouchbase-3.0.0_ubuntu1604_xenial_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.0-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.0-1_amd64.deb "
                               "libcouchbase-dbg_3.0.0-1_amd64.deb "
                               "libcouchbase3-libev_3.0.0-1_amd64.deb "
                               "libcouchbase3-tools_3.0.0-1_amd64.deb "
                               "libcouchbase-dev_3.0.0-1_amd64.deb"]},
                         {"version": "2.10.4",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.4_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.10.4_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.10.4-1_amd64.deb "
                              "libcouchbase2-libevent_2.10.4-1_amd64.deb "
                              "libcouchbase-dev_2.10.4-1_amd64.deb "
                              "libcouchbase2-bin_2.10.4-1_amd64.deb"]},
                         {"version": "2.10.5",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.5_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.10.5_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.10.5-1_amd64.deb "
                              "libcouchbase2-libevent_2.10.5-1_amd64.deb "
                              "libcouchbase-dev_2.10.5-1_amd64.deb "
                              "libcouchbase2-bin_2.10.5-1_amd64.deb"]},
                         {"version": "2.10.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.0_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.10.0_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.10.0-1_amd64.deb "
                              "libcouchbase2-libevent_2.10.0-1_amd64.deb "
                              "libcouchbase-dev_2.10.0-1_amd64.deb "
                              "libcouchbase2-bin_2.10.0-1_amd64.deb"]},
                         {"version": "2.9.5",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.5_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.9.5_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.9.5-1_amd64.deb "
                              "libcouchbase2-libevent_2.9.5-1_amd64.deb "
                              "libcouchbase-dev_2.9.5-1_amd64.deb "
                              "libcouchbase2-bin_2.9.5-1_amd64.deb"]},
                         {"version": "2.10.3",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.3_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.10.3_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.10.3-1_amd64.deb "
                              "libcouchbase2-libevent_2.10.3-1_amd64.deb "
                              "libcouchbase-dev_2.10.3-1_amd64.deb "
                              "libcouchbase2-bin_2.10.3-1_amd64.deb"]},
                         {"version": "2.10.1",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.1_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.10.1_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.10.1-1_amd64.deb "
                              "libcouchbase2-libevent_2.10.1-1_amd64.deb "
                              "libcouchbase-dev_2.10.1-1_amd64.deb "
                              "libcouchbase2-bin_2.10.1-1_amd64.deb"]},
                         {"version": "3.0.0-beta.1",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.0+beta.1_ubuntu1604_xenial_amd64",
                          "package_path": "libcouchbase-3.0.0+beta.1_ubuntu1604_xenial_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.0+beta.1-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.0+beta.1-1_amd64.deb "
                               "libcouchbase-dbg_3.0.0+beta.1-1_amd64.deb "
                               "libcouchbase3-libev_3.0.0+beta.1-1_amd64.deb "
                               "libcouchbase3-tools_3.0.0+beta.1-1_amd64.deb "
                               "libcouchbase-dev_3.0.0+beta.1-1_amd64.deb"]},
                         {"version": "2.9.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.0_ubuntu1604_amd64",
                          "package_path": "libcouchbase-2.9.0_ubuntu1604_amd64",
                          "format": "tar",
                          "install_cmds": [
                              "grep -qxF "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              "/etc/apt/sources.list || echo "
                              "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                              ">> /etc/apt/sources.list",
                              "sudo apt-get update -y",
                              "sudo apt-get install libevent-core-2.1 libev4 -y ",
                              "sudo dpkg -i libcouchbase2-core_2.9.0-1_amd64.deb "
                              "libcouchbase2-libevent_2.9.0-1_amd64.deb "
                              "libcouchbase-dev_2.9.0-1_amd64.deb "
                              "libcouchbase2-bin_2.9.0-1_amd64.deb"]}]


class ClientInstaller:

    def __init__(self, cluster_spec, test_config, options):
        self.test_config = test_config
        self.cluster_spec = cluster_spec
        self.client_settings = self.test_config.client_settings.__dict__
        self.options = options
        self.remote = RemoteHelper(self.cluster_spec, options.verbose)
        self.client_os = RemoteHelper.detect_server_os(self.cluster_spec.workers[0]).lower()

    def client_package_info(self, client, version):
        if client == "libcouchbase":
            for package_info in LIBCOUCHBASE_PACKAGES:
                if package_info["version"] == version and package_info["os"] == self.client_os:
                    return package_info

    @all_clients
    def detect_client_versions(self, client: str):
        if client == "libcouchbase":
            r = run("cbc version 2>&1 | head -n 2 | tail -n 1 | "
                    "awk -F ' ' '{ print $2 }' | "
                    "awk -F '=' '{ print $2 }' | "
                    "awk -F ',' '{ print $1 }'")
        return r

    @all_clients
    def uninstall_clients(self, client: str):
        if client == "libcouchbase":
            run("apt-get remove 'libcouchbase*' -y")

    @all_clients
    def install_clients(self, client: str, version: str):
        client_package_info = self.client_package_info(client, version)
        package = client_package_info['package']
        package_path = client_package_info['package_path']
        package_format = client_package_info['format']
        package_version = client_package_info['version']
        install_cmds = client_package_info['install_cmds']
        if client == "libcouchbase":
            with cd('/tmp'):
                run("rm -rf {}*".format(package))
                run("wget {}/{}/{}.{}".format(LIBCOUCHBASE_BASE_URL,
                                              package_version, package, package_format))
                run("tar xf {}.{}".format(package, package_format))
            with cd("/tmp/{}".format(package_path)):
                for cmd in install_cmds:
                    run(cmd)

    def install(self):
        for client, version in self.client_settings.items():
            if client == "libcouchbase":
                if any([current_version != version
                        for current_version
                        in self.detect_client_versions(client).values()]):
                    self.uninstall_clients(client)
                    logger.info("Installing {} {}".format(client, version))
                    self.install_clients(client, version)
                    logger.info("Successfully installed {} {}".format(client, version))
            elif client == "python_client":
                local("env/bin/pip install couchbase=={}".format(version))


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')
    parser.add_argument('-t', '--test', dest='test_config_fname',
                        required=True,
                        help='path to test test configuration file')
    parser.add_argument('--verbose', dest='verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster settings')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname, override=args.override)
    test_config = TestConfig()
    test_config.parse(args.test_config_fname, override=args.override)

    client_installer = ClientInstaller(cluster_spec, test_config, args)
    client_installer.install()


if __name__ == '__main__':
    main()
