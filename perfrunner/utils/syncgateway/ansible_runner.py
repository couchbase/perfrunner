import logging

from ansible import constants

from perfrunner.utils.syncgateway.ansible_python_runner import Runner

PLAYBOOKS_HOME = "perfrunner/utils/syncgateway/playbooks"


class AnsibleRunner:

    def __init__(self, config):
        self.provisiong_config = config

    def run_ansible_playbook(self, script_name, extra_vars={}, subset=constants.DEFAULT_SUBSET):

        inventory_filename = self.provisiong_config

        playbook_filename = "{}/{}".format(PLAYBOOKS_HOME, script_name)

        runner = Runner(
            inventory_filename=inventory_filename,
            playbook=playbook_filename,
            extra_vars=extra_vars,
            verbosity=0,  # change this to a higher number for -vvv debugging (try 10),
            subset=subset
        )

        stats = runner.run()
        logging.info(stats)

        return len(stats.failures) + len(stats.dark)
