from uuid import uuid4

from celery import Celery
from celery.contrib.methods import task
from fabric.api import cd, run
from fabric import state
from spring.wgen import WorkloadGen

from perfrunner.settings import BROKER_URL, REPO

celery = Celery('workers', broker=BROKER_URL)


class Worker(object):

    def __init__(self, host=None, ssh_username=None, ssh_password=None):
        if host and ssh_username and ssh_password:
            self.is_remote = False

            state.env.user = ssh_username
            state.env.password = ssh_password
            state.env.host_string = host
            state.env.warn_only = True
            state.output.running = False
            state.output.stdout = False

            self.temp_dir = '/tmp/{0}'.format(uuid4().hex[:12])
            self._initialize_project()
        else:
            self.is_remote = True

    def _initialize_project(self):
        with cd(self.temp_dir):
            run('git clone {0}'.format(REPO))
            run('virtualenv perfrunner/env')
            run('perfrunner/env/bin/pip install -r requirements.txt')

    def start(self):
        if self.is_remote:
            with cd('{0}/perfrunner'.format(self.temp_dir)):
                run('env/bin/celery worker -A perfrunner.helpers.worker 2>1 &')

    @task
    def task_run_workload(self, settings, target):
        wg = WorkloadGen(settings, target)
        wg.run()

    def run_workload(self, settings, target):
        if self.is_remote:
            self.task_run_workload.apply(settings, target)
        else:
            self.task_run_workload(settings, target)

    def terminate(self):
        if self.is_remote:
            run('ps auxww|grep "celery worker"|awk "{print $2}"|xargs kill -9')

    def __del__(self):
        if self.is_remote:
            run('rm -fr {0}'.format(self.temp_dir))
