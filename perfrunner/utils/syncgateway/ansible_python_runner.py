import os

import ansible.inventory
from ansible import constants, context
from ansible.executor import playbook_executor
from ansible.inventory.manager import InventoryManager
from ansible.module_utils.common.collections import ImmutableDict
from ansible.parsing.dataloader import DataLoader
from ansible.utils.display import Display
from ansible.vars.manager import VariableManager

from logger import logger


class Options(object):

    """Options class to replace Ansible OptParser."""

    def __init__(self, verbosity=None, inventory=None, listhosts=None,
                 subset=None, module_paths=None, extra_vars=None,
                 forks=None, ask_vault_pass=None, vault_password_files=None,
                 new_vault_password_file=None,
                 output_file=None, tags=None, skip_tags=None, one_line=None,
                 tree=None, become=None, become_exe=None,
                 become_method=None, become_user=None, become_ask_pass=None,
                 ask_pass=None, private_key_file=None, remote_user=None,
                 connection=None, timeout=None, ssh_common_args=None,
                 sftp_extra_args=None, scp_extra_args=None, ssh_extra_args=None,
                 poll_interval=None, seconds=None, check=None,
                 syntax=None, diff=None, force_handlers=None, flush_cache=None,
                 listtasks=None, listtags=None, module_path=None):
        self.verbosity = verbosity
        self.inventory = inventory
        self.listhosts = listhosts
        self.subset = subset
        self.module_paths = module_paths
        self.extra_vars = extra_vars
        self.forks = forks
        self.ask_vault_pass = ask_vault_pass
        self.vault_password_files = vault_password_files
        self.new_vault_password_file = new_vault_password_file
        self.output_file = output_file
        self.one_line = one_line
        self.tree = tree
        self.become = become
        self.become_exe = become_exe
        self.become_method = become_method
        self.become_user = become_user
        self.become_ask_pass = become_ask_pass
        self.ask_pass = ask_pass
        self.private_key_file = private_key_file
        self.remote_user = remote_user
        self.connection = connection
        self.timeout = timeout
        self.ssh_common_args = ssh_common_args
        self.sftp_extra_args = sftp_extra_args
        self.scp_extra_args = scp_extra_args
        self.ssh_extra_args = ssh_extra_args
        self.poll_interval = poll_interval
        self.seconds = seconds
        self.check = check
        self.syntax = syntax
        self.diff = diff
        self.force_handlers = force_handlers
        self.flush_cache = flush_cache
        self.listtasks = listtasks
        self.listtags = listtags
        self.module_path = module_path


class Runner(object):

    def __init__(self, inventory_filename, playbook, extra_vars,
                 verbosity=0, subset=constants.DEFAULT_SUBSET):

        if not os.path.exists(inventory_filename):
            raise Exception("Cannot find inventory_filename: {}. "
                            " Current dir: {}".format(inventory_filename, os.getcwd()))

        if not os.path.exists(playbook):
            raise Exception("Cannot find playbook: {}. "
                            " Current dir: {}".format(playbook, os.getcwd()))

        self.options = Options()
        self.options.verbosity = verbosity
        self.options.connection = 'ssh'  # Need a connection type "smart" or "ssh"
        self.options.subset = subset

        # Propagate defaults from ANSIBLE_CONFIG into options
        self.options.module_path = constants.DEFAULT_MODULE_PATH
        self.options.forks = constants.DEFAULT_FORKS
        self.options.ask_vault_pass = constants.DEFAULT_ASK_VAULT_PASS
        self.options.vault_password_files = [constants.DEFAULT_VAULT_PASSWORD_FILE]
        self.options.become = constants.DEFAULT_BECOME
        self.options.become_exe = constants.DEFAULT_BECOME_EXE
        self.options.become_method = constants.DEFAULT_BECOME_METHOD
        self.options.become_user = constants.DEFAULT_BECOME_USER
        self.options.become_ask_pass = constants.DEFAULT_BECOME_ASK_PASS
        self.options.ask_pass = constants.DEFAULT_ASK_PASS
        self.options.private_key_file = constants.DEFAULT_PRIVATE_KEY_FILE
        self.options.remote_user = constants.DEFAULT_REMOTE_USER
        self.options.timeout = constants.DEFAULT_TIMEOUT
        self.options.poll_interval = constants.DEFAULT_POLL_INTERVAL
        self.options.force_handlers = constants.DEFAULT_FORCE_HANDLERS

        context.CLIARGS = ImmutableDict(tags={},
                                        listtags=False,
                                        listtasks=False,
                                        listhosts=False,
                                        syntax=False,
                                        connection=self.options.connection,
                                        module_path=self.options.module_path,
                                        forks=self.options.forks,
                                        remote_user=self.options.remote_user,
                                        private_key_file=self.options.private_key_file,
                                        ssh_common_args=None,
                                        ssh_extra_args=None,
                                        sftp_extra_args=None,
                                        scp_extra_args=None,
                                        ask_vault_pass=self.options.ask_vault_pass,
                                        vault_password_files=self.options.vault_password_files,
                                        become=self.options.become,
                                        become_method=self.options.become_method,
                                        become_user=self.options.become_user,
                                        become_exe=self.options.become_exe,
                                        verbosity=self.options.verbosity,
                                        become_ask_pass=self.options.ask_pass,
                                        ask_pass=self.options.ask_pass,
                                        timeout=self.options.timeout,
                                        poll_interval=self.options.poll_interval,
                                        force_handlers=self.options.poll_interval,
                                        check=False,
                                        start_at_task=None)

        # Set global verbosity
        self.display = Display()
        self.display.verbosity = self.options.verbosity
        # Executor appears to have it's own
        # verbosity object/setting as well
        playbook_executor.verbosity = self.options.verbosity

        # Become Pass Needed if not logging in as user root
        passwords = {}

        # Gets data from YAML/JSON files
        self.loader = DataLoader()

        # WARNING: this is a dirty hack to avoid a situation where creating multiple
        # instance of this Runner each with it's own Inventory instance was creating
        # a situation where we ended up with different UUID's for hosts and comparisons
        # were failing (see http://bit.ly/1qKmV3x)
        ansible.inventory.manager.HOSTS_PATTERNS_CACHE = {}

        # Set inventory, using most of above objects
        self.inventory = InventoryManager(loader=self.loader,
                                          sources=inventory_filename)

        # All the variables from all the various places
        self.variable_manager = VariableManager(loader=self.loader, inventory=self.inventory)
        self.variable_manager._extra_vars = extra_vars

        self.inventory.subset(self.options.subset)
        self.variable_manager.set_inventory(self.inventory)

        # Setup playbook executor, but don't run until run() called
        logger.info("Running playbook: {}".format(playbook))
        self.pbex = playbook_executor.PlaybookExecutor(
            playbooks=[playbook],
            inventory=self.inventory,
            variable_manager=self.variable_manager,
            loader=self.loader,
            passwords=passwords)

    def run(self):
        # Results of PlaybookExecutor
        self.pbex.run()
        stats = self.pbex._tqm._stats
        return stats
