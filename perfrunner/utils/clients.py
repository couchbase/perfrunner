from argparse import ArgumentParser
from multiprocessing import set_start_method

from fabric.api import cd, local, run

from logger import logger
from perfrunner.helpers.misc import (
    create_build_tuple,
    get_python_sdk_installation,
    run_local_shell_command,
)
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.tableau import TableauTerminalHelper
from perfrunner.remote.context import all_clients
from perfrunner.settings import ClusterSpec, TestConfig

set_start_method("fork")

LIBCOUCHBASE_BASE_URL = "https://github.com/couchbase/libcouchbase/releases/download"
LIBCOUCHBASE_GERRIT_URL = 'https://review.couchbase.org/libcouchbase'

# Examples: lbc_version = 3.3.8, os_version = 'ubuntu1804', os_label = 'focal'
LIBCOUCHBASE_PACKAGES = {
    'ubuntu': {
        "package": "libcouchbase-{lcb_version}_{os_version}_{os_label}_amd64",
        "package_path": "libcouchbase-{lcb_version}_{os_version}_{os_label}_amd64",
        "format": "tar",
        "install_cmds": [
            "grep -qxF "
            "'deb http://us.archive.ubuntu.com/ubuntu/ {os_label} main restricted' "
            "/etc/apt/sources.list || echo "
            "'deb http://us.archive.ubuntu.com/ubuntu/ {os_label} main restricted' "
            ">> /etc/apt/sources.list",
            "sudo apt-get update -y",
            "sudo apt-get install libevent-core-2.1 libev4 -y ",
            "sudo dpkg -i libcouchbase{major}_{lcb_version}-1_amd64.deb "
            "libcouchbase{major}-libevent_{lcb_version}-1_amd64.deb "
            "libcouchbase-dbg_{lcb_version}-1_amd64.deb "
            "libcouchbase{major}-libev_{lcb_version}-1_amd64.deb "
            "libcouchbase{major}-tools_{lcb_version}-1_amd64.deb "
            "libcouchbase-dev_{lcb_version}-1_amd64.deb"
        ]
    }
}

LCB_CUSTOM_DEPS = {
    "ubuntu": [
        "grep -qxF "
        "'deb http://us.archive.ubuntu.com/ubuntu/"
        " bionic main restricted' "
        "/etc/apt/sources.list || echo "
        "'deb http://us.archive.ubuntu.com/ubuntu/"
        " bionic main restricted' "
        ">> /etc/apt/sources.list",
        "sudo apt-get update -y",
        "sudo apt-get install " "libevent-core-2.1 libev4 -y ",
    ]
}


class ClientInstaller:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, options):
        self.test_config = test_config
        self.cluster_spec = cluster_spec
        self.client_settings = self.test_config.client_settings
        self.options = options
        self.remote = RemoteHelper(self.cluster_spec, options.verbose)

        if self.cluster_spec.workers:
            self.client_os = RemoteHelper.detect_client_os(self.cluster_spec.workers[0],
                                                           self.cluster_spec).lower()
        else:
            logger.info('Detecting OS on localhost')
            stdout, _, returncode = run_local_shell_command('egrep "^(ID)=" /etc/os-release')
            if returncode != 0:
                logger.interrupt('Failed to detect OS on localhost')

            self.client_os = stdout.replace("\"", "").split("=")[-1]

        self.rest = RestHelper(self.cluster_spec, self.test_config, options.verbose)

        self.taco_destination = '/var/opt/tableau/tableau_server/data/tabsvc/vizqlserver/Connectors'
        self.jar_destination = '/opt/tableau/tableau_driver/jdbc/'

    @property
    def cb_version(self):
        return create_build_tuple(self.rest.get_version(host=next(self.cluster_spec.masters)))

    @all_clients
    def detect_libcouchbase_versions(self):
        return run("cbc version 2>&1 | head -n 2 | tail -n 1 | "
                   "awk -F ' ' '{ print $2 }' | "
                   "awk -F '=' '{ print $2 }' | "
                   "awk -F ',' '{ print $1 }'")

    @all_clients
    def uninstall_lcb(self):
        # if any libcouchbase packages are installed, uninstall them; otherwise do nothing
        run("(dpkg-query -l | grep -q libcouchbase) && apt-get remove 'libcouchbase*' -y || :")

    @all_clients
    def install_libcouchbase(self, version: str):
        client_package_info = LIBCOUCHBASE_PACKAGES.get(self.client_os)

        if not client_package_info:
            raise Exception(f"LCB package v{version} not found for {self.client_os} os")

        os_label = run("cat /etc/os-release | grep UBUNTU_CODENAME").split("=")[1]
        os_id = run("cat /etc/os-release | grep ^ID=").split("=")[1]
        os_version_id = run("cat /etc/os-release | grep VERSION_ID=").split("=")[1]
        os_version_id = os_version_id.replace(".", "").replace('"', "")
        os_version = f"{os_id}{os_version_id}"

        package = client_package_info["package"].format(
            lcb_version=version, os_version=os_version, os_label=os_label
        )
        package_path = client_package_info["package_path"].format(
            lcb_version=version, os_version=os_version, os_label=os_label
        )
        package_format = client_package_info["format"]
        install_cmds = client_package_info["install_cmds"]

        with cd("/tmp"):
            run(f"rm -rf {package}*")
            run(f"wget {LIBCOUCHBASE_BASE_URL}/{version}/{package}.{package_format}")
            run(f"tar xf {package}.{package_format}")
        with cd(f"/tmp/{package_path}"):
            for cmd in install_cmds:
                cmd = cmd.format(os_label=os_label, lcb_version=version, major=version[0])
                run(cmd)

    @all_clients
    def install_lcb_from(self, version: str, source: str = "commit"):
        dep_cmds = LCB_CUSTOM_DEPS[self.client_os]
        for cmd in dep_cmds:
            run(cmd)
        with cd('/tmp'):
            run("rm -rf libcouchbase_custom")
            run("mkdir libcouchbase_custom")
        with cd('/tmp/libcouchbase_custom'):
            run('git clone https://github.com/couchbase/libcouchbase.git')
        with cd('/tmp/libcouchbase_custom/libcouchbase'):
            if source == "gerrit":
                run(f"git pull {LIBCOUCHBASE_GERRIT_URL} {version}")
            else:
                _, _, commit_id = version.split(":")
                run(f"git checkout {commit_id}")
            run('mkdir build')
        with cd('/tmp/libcouchbase_custom/libcouchbase/build'):
            run('apt-get install cmake libevent-dev libevent-core-2.1 libev4 -y')
            run('../cmake/configure')
            run('make')

    def detect_python_client_version(self):
        return local("env/bin/pip freeze | grep ^couchbase | awk -F '==|@' '{ print $2 }'",
                     capture=True)

    def install_python_client(self, version: str):
        package = get_python_sdk_installation(version)
        local(f'PYCBC_USE_CPM_CACHE=OFF env/bin/pip install {package} --no-cache-dir')

    def uninstall_tableau_connectors(self):
        local(f'rm -rf {self.jar_destination}/*couchbase*')
        local(f'rm -rf {self.taco_destination}/*couchbase*.taco')

    def install_cdata_tableau_connector(self):
        logger.info('Installing CData Tableau Connector')

        jarfile = 'cdata.tableau.couchbase.jar'
        tacofile = 'cdata.couchbase.taco'
        licensefile = 'cdata.tableau.couchbase.lic'
        cdata_dir = '/opt/cdata/lib/'

        self.tableau_term.copy_file(f'{cdata_dir}/{jarfile}', self.jar_destination)
        self.tableau_term.copy_file(f'{cdata_dir}/{licensefile}', self.jar_destination)
        self.tableau_term.copy_file(f'{cdata_dir}/{tacofile}', self.taco_destination)

        version = self.tableau_term.get_connector_version()

        logger.info(f'CData Tableau Connector {version} has been installed.')

    def install_couchbase_tableau_connector(self, full_version: str):
        version, build = full_version.split('-')

        url = (
            'http://172.23.126.166/builds/latestbuilds/couchbase-tableau-connector/'
            '{0}/{1}/couchbase-tableau-connector-{0}-{1}.zip'
        ).format(version, build)

        logger.info(f'Installing Couchbase Tableau Connector using URL: {url}')
        archive = 'tableau_connector.zip'
        local(f'wget -nv {url} -O {archive}')

        jarfile = 'couchbase-jdbc-driver-*.jar'
        tacofile = 'couchbase-analytics-*.taco'
        local(f'unzip {archive} {jarfile} {tacofile}')

        self.tableau_term.copy_file(jarfile, self.jar_destination)
        self.tableau_term.copy_file(tacofile, self.taco_destination)

        tms_config_cmds = [
            f'configuration set -frc -k native_api.connect_plugins_path -v {self.taco_destination}',
            'configuration set -k JdbcDriverCustomLoad -v false --force-keys',
        ]

        for cmd in tms_config_cmds:
            self.tableau_term.run_tsm_command(cmd)

        logger.info(f'Couchbase Tableau Connector {full_version} has been installed.')

    def install(self):
        lcb_version = self.client_settings.libcouchbase
        py_version = self.client_settings.python_client
        logger.info(f'Desired clients: lcb={lcb_version}, py={py_version}')

        # Check if we are using Capella to avoid evaluating the server version if we are using
        # Capella Columnar, as the "self.rest.get_version(...)" method doesn't work for
        # Capella Columnar yet...
        mb45563_is_hit = ((not self.cluster_spec.capella_infrastructure) and
                          self.cb_version >= (7, 1, 0, 1745) and
                          self.cb_version < (7, 1, 0, 1807))

        if not py_version:
            logger.info("No python SDK version provided. "
                        "Defaulting to version specified in requirements.txt")
        elif mb45563_is_hit and create_build_tuple(py_version) < (3, 2, 0):
            # SDK compatibility changed with 7.1.0-1745
            # see https://issues.couchbase.com/browse/MB-45563
            logger.warn("python SDK >= 3.2.0 required for Couchbase Server builds "
                        "7.1.0-1745 <= (build) < 7.1.0-1807. "
                        "Upgrading python SDK version to 3.2.3")
            py_version = "3.2.3"

        if not lcb_version and self.cluster_spec.workers:
            logger.info("No libcouchbase version provided. Uninstalling libcouchbase.")
            self.uninstall_lcb()
        elif mb45563_is_hit and create_build_tuple(lcb_version) < (3, 2, 0):
            # SDK compatibility changed with 7.1.0-1745
            # see https://issues.couchbase.com/browse/MB-45563
            logger.warn("libcouchbase >= 3.2.0 required for Couchbase Server builds "
                        "7.1.0-1745 <= (build) < 7.1.0-1807. "
                        "Upgrading libcouchbase version to 3.2.3")
            lcb_version = "3.2.3"

        if py_version and py_version.split('.')[0] == "2" and \
           lcb_version and lcb_version.split('.')[0] != "2":
            raise Exception("libcouchbase version 2.x.x must be specified when python_client=2.x.x")

        # Install LCB
        if lcb_version and self.cluster_spec.workers:
            installed_versions = self.detect_libcouchbase_versions()

            if any(v != lcb_version for v in installed_versions.values()):
                if any(installed_versions.values()):
                    logger.info("Uninstalling libcouchbase")
                    self.uninstall_lcb()

                else:
                    logger.info("libcouchbase is not installed")

                logger.info(f'Installing libcouchbase {lcb_version}')

                if 'commit' in lcb_version:
                    self.install_lcb_from(lcb_version, "commit")
                elif 'changes' in lcb_version:
                    self.install_lcb_from(lcb_version, "gerrit")
                else:
                    self.install_libcouchbase(lcb_version)
            else:
                logger.info("Clients already have desired libcouchbase versions.")

        if self.cluster_spec.workers:
            detected = self.detect_libcouchbase_versions()
            for ip, version in detected.items():
                logger.info(f'\t{ip}:\t{version}')

        # Install Python SDK
        if py_version:
            logger.info(f'Installing python_client {py_version}')
            self.install_python_client(py_version)

        detected = self.detect_python_client_version()
        logger.info(f'Python client detected (pip freeze): {detected}')

        # Install Tableau Connector
        tableau_connector_vendor = self.test_config.tableau_settings.connector_vendor
        if tableau_connector_vendor and self.cluster_spec.workers:
            self.tableau_term = TableauTerminalHelper(self.test_config)

            self.uninstall_tableau_connectors()

            if tableau_connector_vendor == 'couchbase':
                connector_version = self.client_settings.tableau_connector
                logger.info('Desired Tableau Connector: vendor=couchbase, '
                            f'version={connector_version}')
                self.install_couchbase_tableau_connector(connector_version)
            elif tableau_connector_vendor == 'cdata':
                logger.info("Desired Tableau Connector: vendor=cdata")
                self.install_cdata_tableau_connector()
            else:
                logger.info('No Tableau Connector required.')

        # Install pymongo
        if (
            self.cluster_spec.goldfish_infrastructure
            and self.test_config.columnar_kafka_links_settings.link_source == "MONGODB"
        ):
            logger.info('Installing pymongo for Goldfish Kafka Links')
            run_local_shell_command("env/bin/pip install pymongo --no-cache-dir")


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
