import configparser
import os
import sys

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

    if "-" not in _build:
        base_url = "http://172.23.126.166/builds/releases/mobile/" \
                   "couchbase-sync-gateway/{}".format(_build)
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    elif 'toy' in _build:
        base_url = "http://172.23.126.166/builds/latestbuilds/sync_gateway/toys/{}"\
            .format(_build.split("/")[7])
        _build = _build.split("_")[2]
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    else:
        v, b = _build.split("-")
        base_url = "http://172.23.126.166/builds/latestbuilds/sync_gateway/{}/{}".format(v, b)
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)

    _cluster_full_path = os.path.abspath(_cluster_path)
    _config_full_path = os.path.abspath(_config_path)

    parser = configparser.ConfigParser(allow_no_value=True)
    parser.read(_cluster_full_path)
    confdict = {section: dict(parser.items(section)) for section in parser.sections()}
    server_ip = confdict.get('servers', confdict.get('couchbase_servers', None))
    if server_ip is None:
        server_ip = confdict.get('servers', confdict.get('clusters', None))

    with open(_config_full_path, 'r') as file:
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace('couchbase://172.23.100.190', 'couchbase://{}'.format(
        list(server_ip.keys())[0]))

    # Write the file out again
    with open(_config_full_path, 'w') as file:
        file.write(filedata)

    playbook_vars = dict()
    playbook_vars["sync_gateway_config_filepath"] = _config_full_path
    playbook_vars["couchbase_sync_gateway_package_base_url"] = base_url
    playbook_vars["couchbase_sync_gateway_package"] = sg_package_name
    playbook_vars["couchbase_sg_accel_package"] = accel_package_name

    if _uninstall:
        playbook = "remove-previous-installs.yml"
    else:
        playbook = "install-sync-gateway-package.yml"
    ansible_runner = AnsibleRunner(_cluster_path)

    status = ansible_runner.run_ansible_playbook(playbook, extra_vars=playbook_vars)
    if status != 0:
        logger.info("Failed to install sync_gateway package")
        sys.exit(1)


if __name__ == '__main__':
    main()
