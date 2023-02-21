import configparser
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import requests

from logger import logger
from perfrunner.utils.syncgateway.ansible_runner import AnsibleRunner


def main():

    _uninstall = False
    for i, item in enumerate(sys.argv):
        if item == "-build":
            _build = sys.argv[i + 1]
        elif item == "-cluster":
            _cluster_path = sys.argv[i + 1]
        elif item == "-config":
            _config_path = sys.argv[i + 1]
        elif item == "--uninstall":
            _uninstall = True

    if 'toy' in _build:
        base_url = "http://172.23.126.166/builds/latestbuilds/sync_gateway/toys/{}"\
            .format(_build.split("/")[7])
        _build = "3.2.0-{}".format(_build.split("/")[7])
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.deb".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.deb".format(_build)
    elif "-" not in _build:
        base_url = "http://172.23.126.166/builds/releases/mobile/" \
                   "couchbase-sync-gateway/{}".format(_build)
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.deb".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.deb".format(_build)
    else:
        v, b = _build.split("-")
        base_url = "http://172.23.126.166/builds/latestbuilds/sync_gateway/{}/{}".format(v, b)
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.deb".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.deb".format(_build)

    _cluster_full_path = os.path.abspath(_cluster_path)
    _config_full_path = os.path.abspath(_config_path)

    playbook_vars = dict()
    playbook_vars["sync_gateway_config_filepath"] = _config_full_path
    playbook_vars["couchbase_sync_gateway_package_base_url"] = base_url
    playbook_vars["couchbase_sync_gateway_package"] = sg_package_name
    playbook_vars["couchbase_sg_accel_package"] = accel_package_name

    installer = BootstrapInstaller()
    if _uninstall or installer.use_legacy_installer(_config_path):
        installer = LegacyInstaller()

    status = installer.install(_cluster_full_path, _cluster_path,
                               _config_full_path, playbook_vars, _uninstall)
    if status != 0:
        logger.info("Failed to install sync_gateway package")
        sys.exit(1)


class LegacyInstaller:

    def _get_server_and_sync_ips(self, cluster_path: str) -> tuple[str, list[str]]:
        parser = configparser.ConfigParser(allow_no_value=True)
        parser.read(cluster_path)
        if parser.has_section('couchbase_servers'):
            server_ip = self._to_list(parser.items('couchbase_servers'))[0]
        else:
            server_ip = self._to_list(parser.items('clusters'))[0]
        sync_ips = self._to_list(parser.items('syncgateways'))
        return server_ip, sync_ips

    def _to_list(self, items) -> list:
        return [item for item, _ in items]

    def write_config_ini(self, ips_to_remove, ips_to_add, config_path):
        with open(config_path, 'r+') as f:
            s = f.read()
            s = s.replace("\n".join(ips_to_remove), "\n".join(ips_to_add))
            f.seek(0)
            f.write(s)

    def install(self, cluster_path, cluster_file_name, config_path,
                playbook_vars, uninstall) -> int:
        logger.info("Running a legacy installer")
        server_ip, _ = self._get_server_and_sync_ips(cluster_path)
        with open(config_path, 'r') as file:
            filedata = file.read()

        # Replace the target string
        filedata = filedata.replace('couchbase://172.23.100.190',
                                    'couchbase://{}'.format(server_ip))

        # Write the file out again
        with open(config_path, 'w') as file:
            file.write(filedata)

        return self.run_playbook(cluster_file_name, playbook_vars, uninstall)

    def run_playbook(self, cluster_file_name, playbook_vars, uninstall) -> int:
        if uninstall:
            playbook = "remove-previous-installs.yml"
        else:
            playbook = "install-sync-gateway-package.yml"
        ansible_runner = AnsibleRunner(cluster_file_name)

        return ansible_runner.run_ansible_playbook(playbook, extra_vars=playbook_vars)


class BootstrapInstaller(LegacyInstaller):

    def get_bootstrap_and_import_nodes(self, config_path: str) -> tuple[str, int]:
        config = self._get_json(config_path)
        import_nodes = int(config.get('import_nodes', 0))
        return config.get('bootstrap_path'), import_nodes

    def add_server_ip(self, bootstrap_path: str, server_ip: str):
        config = self._get_json(bootstrap_path)
        config['bootstrap']['server'] = "couchbase://{}".format(server_ip)

        with open(bootstrap_path, "w") as bootstrap_f:
            json.dump(config, bootstrap_f)

    def set_import_docs(self, config_path: str, import_docs: bool):
        config = self._get_json(config_path)
        databases = config.get('databases', {})
        for _, db_config in databases.items():
            db_config['import_docs'] = import_docs

        with open(config_path, "w") as db_f:
            json.dump(config, db_f)

    def set_group_id(self, bootstrap_path: str, group_id: str):
        config = self._get_json(bootstrap_path)
        config['bootstrap']['group_id'] = group_id
        with open(bootstrap_path, "w") as bootstrap_f:
            json.dump(config, bootstrap_f)

    def _remove_non_import_nodes(self, config_path: str, import_nodes: int, sync_ips):
        """Modify the config file and remove all the non-import sync-gateway nodes.

        This can then be used to do the installation with the import group.
        """
        self.write_config_ini(sync_ips, sync_ips[0:import_nodes], config_path)

    def _remove_import_nodes(self, config_path: str, import_nodes: int, sync_ips):
        """Modify the config file and only leave the non-import sync-gateway nodes.

        Return the new node ip for non-import group installation
        """
        self.write_config_ini(sync_ips[0:import_nodes], sync_ips[import_nodes:], config_path)

    def create_dbs(self, db_path: str, sync_ip: str):
        config = self._get_json(db_path).get('databases', {})
        with ThreadPoolExecutor() as executor:
            executor.map(self._create_db_rest, self._produce_db_config(config, sync_ip))

    def _produce_db_config(self, config: dict, sync_ip: str) -> tuple[str, dict, str]:
        for db_name, db_config in config.items():
            yield db_name, db_config, sync_ip

    def _create_db_rest(self, db_config: dict):
        db_name, config, sync_ip = db_config
        logger.info('Creating database: {} using {}'.format(db_name, sync_ip))
        resp = requests.put('http://{}:4985/{}/'.format(sync_ip, db_name),
                            headers={'Content-Type': 'application/json'},
                            data=json.dumps(config), verify=False)
        resp.raise_for_status()

    def _get_json(self, path: str) -> dict:
        with open(path) as file:
            return json.load(file)

    def install(self, cluster_path: str, cluster_file_name: str, config_path: str,
                playbook_vars: dict, uninstall: bool) -> int:
        logger.info("Running a bootstrap installer")
        server_ip, sync_ips = self._get_server_and_sync_ips(cluster_path)
        bootstrap_path, import_nodes = self.get_bootstrap_and_import_nodes(config_path)
        playbook_vars["sync_gateway_config_filepath"] = os.path.abspath(bootstrap_path)
        self.add_server_ip(bootstrap_path, server_ip)

        # If {import_nodes} is specified, we need 2 groups and 2 installations
        # The design here is to use the first {import_nodes} as import and false otherwise
        if import_nodes and not uninstall:
            # Install the first sgw group with import
            logger.info("Multi-group setup. Installing importOn group")
            self.set_group_id(bootstrap_path, 'importOn')
            self.set_import_docs(config_path, True)
            self._remove_non_import_nodes(cluster_path, import_nodes, sync_ips)
            status = self._start_and_create_dbs(cluster_file_name, playbook_vars, uninstall,
                                                sync_ips[0], config_path)

            # modify cluster, sync_ip and new group
            self.set_group_id(bootstrap_path, 'importOff')
            self.set_import_docs(config_path, False)
            self._remove_import_nodes(cluster_path, import_nodes, sync_ips)
            logger.info("Installing importOff group")
            status = self._start_and_create_dbs(cluster_file_name, playbook_vars, uninstall,
                                                sync_ips[import_nodes], config_path)
            # Reset the cluster file to original values
            self.write_config_ini(sync_ips[import_nodes:], sync_ips, cluster_path)
            return status
        else:
            return self._start_and_create_dbs(cluster_file_name, playbook_vars, uninstall,
                                              sync_ips[0], config_path)

    def _start_and_create_dbs(self, cluster_file_name: str, playbook_vars: dict,
                              uninstall: bool, sync_ip: str, config_path: str) -> int:
        # Use bootstrap to start sync-gateway
        status = self.run_playbook(cluster_file_name, playbook_vars, uninstall)
        # Create the database using the admin REST API
        if status == 0 and not uninstall:
            self.create_dbs(config_path, sync_ip)

        return status

    def use_legacy_installer(self, config_path: str) -> bool:
        try:
            config = self._get_json(config_path)
            return config.get('bootstrap_path') is None
        except ValueError:
            return True


if __name__ == '__main__':
    main()
