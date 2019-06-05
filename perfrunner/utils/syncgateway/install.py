import os
import re
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

    if re.match('2.0.0', _build):
        base_url = "http://172.23.120.24/builds/releases/mobile/couchbase-sync-gateway/2.0.0"
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    elif re.match('2.1.0', _build):
        base_url = "http://172.23.120.24/builds/releases/mobile/couchbase-sync-gateway/2.1.0"
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    elif re.match('2.1.1', _build):
        base_url = "http://172.23.120.24/builds/releases/mobile/couchbase-sync-gateway/2.1.1"
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    elif re.match('1.5.1', _build):
        base_url = "http://172.23.120.24/builds/releases/mobile/couchbase-sync-gateway/1.5.1"
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    elif 'toy' in _build:
        base_url = "http://mobile.jenkins.couchbase.com/view/Sync_Gateway/job/" \
                   "sgw-toy-build/{}/artifact".format(_build.split("/")[7])
        _build = _build.split("_")[2]
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)
    else:
        v, b = _build.split("-")
        base_url = "http://172.23.120.24/builds/latestbuilds/sync_gateway/{}/{}".format(v, b)
        sg_package_name = "couchbase-sync-gateway-enterprise_{}_x86_64.rpm".format(_build)
        accel_package_name = "couchbase-sg-accel-enterprise_{}_x86_64.rpm".format(_build)

    _config_full_path = os.path.abspath(_config_path)

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
