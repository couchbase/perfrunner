from argparse import ArgumentParser

from fabric.api import cd, local, run

from logger import logger
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import all_clients
from perfrunner.settings import ClusterSpec, TestConfig

LIBCOUCHBASE_BASE_URL = "https://github.com/couchbase/libcouchbase/releases/download"
LIBCOUCHBASE_PACKAGES = [{"version": "2.9.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.0_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.9.0_ubuntu1804_amd64",
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
                              "libcouchbase2-bin_2.9.0-1_amd64.deb"]},
                         {"version": "2.9.3",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.3_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.9.3_ubuntu1804_amd64",
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
                         {"version": "2.9.5",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.9.5_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.9.5_ubuntu1804_amd64",
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
                         {"version": "2.10.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.0_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.10.0_ubuntu1804_amd64",
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
                         {"version": "2.10.1",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.1_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.10.1_ubuntu1804_amd64",
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
                         {"version": "2.10.3",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.3_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.10.3_ubuntu1804_amd64",
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
                         {"version": "2.10.4",
                          "os": "ubuntu",
                          "package": "libcouchbase-2.10.4_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.10.4_ubuntu1804_amd64",
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
                          "package": "libcouchbase-2.10.5_ubuntu1804_amd64",
                          "package_path": "libcouchbase-2.10.5_ubuntu1804_amd64",
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
                         {"version": "3.0.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.0_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.0.0_ubuntu1804_bionic_amd64",
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
                         {"version": "3.0.1",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.1_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.0.1_ubuntu1804_bionic_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.1-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.1-1_amd64.deb "
                               "libcouchbase-dbg_3.0.1-1_amd64.deb "
                               "libcouchbase3-libev_3.0.1-1_amd64.deb "
                               "libcouchbase3-tools_3.0.1-1_amd64.deb "
                               "libcouchbase-dev_3.0.1-1_amd64.deb"]},
                         {"version": "3.0.2",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.2_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.0.2_ubuntu1804_bionic_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.2-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.2-1_amd64.deb "
                               "libcouchbase-dbg_3.0.2-1_amd64.deb "
                               "libcouchbase3-libev_3.0.2-1_amd64.deb "
                               "libcouchbase3-tools_3.0.2-1_amd64.deb "
                               "libcouchbase-dev_3.0.2-1_amd64.deb"]},
                         {"version": "3.0.7",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.0.7_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.0.7_ubuntu1804_bionic_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.0.7-1_amd64.deb "
                               "libcouchbase3-libevent_3.0.7-1_amd64.deb "
                               "libcouchbase-dbg_3.0.7-1_amd64.deb "
                               "libcouchbase3-libev_3.0.7-1_amd64.deb "
                               "libcouchbase3-tools_3.0.7-1_amd64.deb "
                               "libcouchbase-dev_3.0.7-1_amd64.deb"]},
                         {"version": "3.2.0",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.2.0_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.2.0_ubuntu1804_bionic_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.2.0-1_amd64.deb "
                               "libcouchbase3-libevent_3.2.0-1_amd64.deb "
                               "libcouchbase-dbg_3.2.0-1_amd64.deb "
                               "libcouchbase3-libev_3.2.0-1_amd64.deb "
                               "libcouchbase3-tools_3.2.0-1_amd64.deb "
                               "libcouchbase-dev_3.2.0-1_amd64.deb"]},
                         {"version": "3.2.2",
                          "os": "ubuntu",
                          "package": "libcouchbase-3.2.2_ubuntu1804_bionic_amd64",
                          "package_path": "libcouchbase-3.2.2_ubuntu1804_bionic_amd64",
                          "format": "tar",
                          "install_cmds":
                              ["grep -qxF "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               "/etc/apt/sources.list || echo "
                               "'deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted' "
                               ">> /etc/apt/sources.list",
                               "sudo apt-get update -y",
                               "sudo apt-get install libevent-core-2.1 libev4 -y ",
                               "sudo dpkg -i libcouchbase3_3.2.2-1_amd64.deb "
                               "libcouchbase3-libevent_3.2.2-1_amd64.deb "
                               "libcouchbase-dbg_3.2.2-1_amd64.deb "
                               "libcouchbase3-libev_3.2.2-1_amd64.deb "
                               "libcouchbase3-tools_3.2.2-1_amd64.deb "
                               "libcouchbase-dev_3.2.2-1_amd64.deb"]}
                         ]

LCB_CUSTOM_DEPS = {
    '3.0.0':
        {'ubuntu': ["grep -qxF "
                    "'deb http://us.archive.ubuntu.com/ubuntu/"
                    " bionic main restricted' "
                    "/etc/apt/sources.list || echo "
                    "'deb http://us.archive.ubuntu.com/ubuntu/"
                    " bionic main restricted' "
                    ">> /etc/apt/sources.list",
                    "sudo apt-get update -y",
                    "sudo apt-get install "
                    "libevent-core-2.1 libev4 -y "]
         },
    '3.2.0':
        {'ubuntu': ["grep -qxF "
                    "'deb http://us.archive.ubuntu.com/ubuntu/"
                    " bionic main restricted' "
                    "/etc/apt/sources.list || echo "
                    "'deb http://us.archive.ubuntu.com/ubuntu/"
                    " bionic main restricted' "
                    ">> /etc/apt/sources.list",
                    "sudo apt-get update -y",
                    "sudo apt-get install "
                    "libevent-core-2.1 libev4 -y "]
         }
}


class ClientInstaller:

    def __init__(self, cluster_spec, test_config, options):
        self.test_config = test_config
        self.cluster_spec = cluster_spec
        self.client_settings = self.test_config.client_settings.__dict__
        self.options = options
        self.remote = RemoteHelper(self.cluster_spec, options.verbose)
        self.client_os = RemoteHelper.detect_client_os(self.cluster_spec.workers[0],
                                                       self.cluster_spec).lower()

    @all_clients
    def detect_libcouchbase_versions(self):
        return run("cbc version 2>&1 | head -n 2 | tail -n 1 | "
                   "awk -F ' ' '{ print $2 }' | "
                   "awk -F '=' '{ print $2 }' | "
                   "awk -F ',' '{ print $1 }'")

    def detect_python_client_version(self):
        return local("env/bin/pip freeze | grep ^couchbase | awk -F '==' '{ print $2 }'",
                     capture=True)

    @all_clients
    def uninstall_clients(self, client: str):
        if client == "libcouchbase":
            run("apt-get remove 'libcouchbase*' -y")

    @all_clients
    def install_libcouchbase(self, version: str):
        client_package_info = None
        for package_info in LIBCOUCHBASE_PACKAGES:
            if package_info["version"] == version and package_info["os"] == self.client_os:
                client_package_info = package_info

        if client_package_info is None:
            raise Exception("invalid client version or os")
        package = client_package_info['package']
        package_path = client_package_info['package_path']
        package_format = client_package_info['format']
        package_version = client_package_info['version']
        install_cmds = client_package_info['install_cmds']
        os_version = run('cat /etc/os-release | grep UBUNTU_CODENAME')
        os_version = os_version.split('=')[1]
        if os_version == 'bionic':
            package = package.replace('ubuntu1604', 'ubuntu1804')
            package = package.replace('xenial', 'bionic')
            package_path = package_path.replace('ubuntu1604', 'ubuntu1804')
            package_path = package_path.replace('xenial', 'bionic')
        with cd('/tmp'):
            run("rm -rf {}*".format(package))
            run("wget {}/{}/{}.{}".format(LIBCOUCHBASE_BASE_URL, package_version, package,
                                          package_format))
            run("tar xf {}.{}".format(package, package_format))
        with cd("/tmp/{}".format(package_path)):
            for cmd in install_cmds:
                run(cmd)

    @all_clients
    def install_lcb_from_commit(self, version: str):
        _, version, commit_id = version.split(":")
        dep_cmds = LCB_CUSTOM_DEPS[version][self.client_os]
        for cmd in dep_cmds:
            run(cmd)
        with cd('/tmp'):
            run("rm -rf libcouchbase_custom")
            run("mkdir libcouchbase_custom")
        with cd('/tmp/libcouchbase_custom'):
            run('git clone https://github.com/couchbase/libcouchbase.git')
        with cd('/tmp/libcouchbase_custom/libcouchbase'):
            run('git checkout {}'.format(commit_id))
            run('mkdir build')
        with cd('/tmp/libcouchbase_custom/libcouchbase/build'):
            run('apt-get install cmake libevent-dev libevent-core-2.1 libev4 -y')
            run('../cmake/configure')
            run('make')

    def install_python_client(self, version: str):
        if not ('review.couchbase.org' in version or 'github.com' in version):
            version = "couchbase=={}".format(version)

        local("env/bin/pip install {} --no-cache-dir".format(version))

    def install(self):
        lcb_version = self.client_settings['libcouchbase']
        py_version = self.client_settings['python_client']
        logger.info("Desired clients: lcb={}, py={}".format(lcb_version, py_version))

        if py_version.split('.')[0] == "2" and not lcb_version:
            raise Exception("libcouchbase version must be specified when python_client=2.x.x")

        if lcb_version:
            installed_versions = self.detect_libcouchbase_versions()

            if any(v != lcb_version for v in installed_versions.values()):
                logger.info("Uninstalling libcouchbase")
                self.uninstall_clients('libcouchbase')

                logger.info("Installing libcouchbase {}".format(lcb_version))

                if 'commit' in lcb_version:
                    self.install_lcb_from_commit(lcb_version)
                else:
                    self.install_libcouchbase(lcb_version)

                detected = self.detect_libcouchbase_versions()
                logger.info("libcouchbase versions detected after installation: ")
                for ip, version in detected.items():
                    logger.info("\t{}:\t{}".format(ip, version))
            else:
                logger.info("Clients already have desired libcouchbase versions: ")
                for ip, version in installed_versions.items():
                    logger.info("\t{}:\t{}".format(ip, version))

        if py_version:
            logger.info("Installing python_client {}".format(py_version))
            self.install_python_client(py_version)
            detected = self.detect_python_client_version()
            logger.info("Python client detected after installation (pip freeze): {}"
                        .format(detected))


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
