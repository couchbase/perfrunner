import base64
import datetime
import json
import os
import subprocess
import time

import requests
import yaml

from logger import logger
from perfrunner.helpers.config_files import (
    CAOCouchbaseBackupFile,
    CAOHorizontalAutoscalerFile,
)
from perfrunner.remote import Remote
from perfrunner.settings import ClusterSpec, SyncgatewaySettings


class RemoteKubernetes(Remote):

    PLATFORM = 'kubernetes'

    def __init__(self, cluster_spec: ClusterSpec):
        super().__init__(cluster_spec)
        if cluster_spec.cloud_provider == 'openshift':
            self.k8s_client = self._oc
        else:
            self.k8s_client = self._kubectl

        self.kube_config_path = "cloud/infrastructure/generated/kube_configs/k8s_cluster_1"
        with open(cluster_spec.generated_cloud_config_path) as f:
            self.kube_config_path = json.load(f).get("kubeconfigs", [self.kube_config_path])[0]

    @property
    def _git_access_token(self):
        return os.environ.get('GITHUB_ACCESS_TOKEN', None)

    @property
    def _git_username(self):
        return os.environ.get('GITHUB_USERNAME', None)

    def run_subprocess(self, params, split_lines=True, max_attempts=3):
        attempt = 1
        while attempt <= max_attempts:
            if attempt > 1:
                time.sleep(1)
            try:
                res = subprocess.run(params,
                                     check=True,
                                     stderr=subprocess.PIPE,
                                     stdout=subprocess.PIPE)

                if split_lines:
                    return res.stdout.splitlines()
                else:
                    return res.stdout
            except Exception as ex:
                logger.info(ex.stderr.decode('ascii'))
            finally:
                attempt += 1
        raise Exception("max attempts exceeded")

    # kubectl client
    def _kubectl(self, params, split_lines=True, max_attempts=3):
        """Kubectl client helper. Do not use directly. Use {k8s_client} instead."""
        params = params.split()
        if params[0] == 'exec':
            params = params[0:5] + [" ".join(params[5::])]
        params = ['kubectl', '--kubeconfig', self.kube_config_path] + params
        return self.run_subprocess(params, split_lines=split_lines, max_attempts=max_attempts)

    # openshift client (oc)
    def _oc(self, params, split_lines=True, max_attempts=3):
        """Openshift client helper. Do not use directly. Use {k8s_client} instead."""
        params = ['oc'] + params.split()
        return self.run_subprocess(params, split_lines=split_lines, max_attempts=max_attempts)

    def kubectl_exec(self, pod, params):
        return self.k8s_client("exec {} -- bash -c {}".format(pod, params))

    def create_namespace(self, name):
        self.k8s_client("create namespace {}".format(name))

    def delete_namespace(self, name):
        self.k8s_client("delete namespace {}".format(name))

    def get_pods(self, namespace="default"):
        raw_pods = self.k8s_client(
            "get pods -o json -n {}".format(namespace),
            split_lines=False
        )
        pods = json.loads(raw_pods.decode('utf8'))
        return pods["items"]

    def get_services(self, namespace="default"):
        raw_svcs = self.k8s_client(
            "get svc -o json -n {}".format(namespace),
            split_lines=False
        )
        svcs = json.loads(raw_svcs.decode('utf8'))
        return svcs["items"]

    def get_nodes(self):
        raw_nodes = self.k8s_client(
            "get nodes -o json",
            split_lines=False
        )
        nodes = json.loads(raw_nodes.decode('utf8'))
        return nodes["items"]

    def get_storage_classes(self):
        raw_sc = self.k8s_client(
            "get sc -o json",
            split_lines=False
        )
        sc = json.loads(raw_sc.decode('utf8'))
        return sc

    def get_jobs(self):
        raw_jobs = self.k8s_client(
            "get jobs -o json",
            split_lines=False
        )
        jobs = json.loads(raw_jobs.decode('utf8'))
        return jobs

    def get_cronjobs(self):
        raw_cronjobs = self.k8s_client(
            "get cronjobs -o json",
            split_lines=False
        )
        cronjobs = json.loads(raw_cronjobs.decode('utf8'))
        return cronjobs

    def delete_storage_class(self, storage_class, ignore_errors=True):
        try:
            self.k8s_client("delete sc {}".format(storage_class))
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def get_worker_pods(self):
        return [
            pod["metadata"]["name"]
            for pod in self.get_pods()
            if "worker" in pod.get("metadata", {}).get("name", "")]

    def create_secret(self, secret_name, secret_type, file):
        if secret_type == 'docker':
            cmd = "create secret generic {} " \
                  "--from-file=.dockerconfigjson={} " \
                  "--type=kubernetes.io/dockerconfigjson".format(secret_name, file)
        elif secret_type == 'generic':
            cmd = "create secret generic {} " \
                  "--from-file={}".format(secret_name, file)
        elif secret_type == 'tls':
            cmd = "create secret tls {} " \
                  "--from-file={}".format(secret_name, file)
        elif secret_type == 'docker-registry':
            cmd = "create secret docker-registry {} " \
                  "--docker-server=ghcr.io " \
                  "--docker-username={} " \
                  "--docker-password={} " \
                  .format(secret_name, self._git_username, self._git_access_token)
        else:
            raise Exception('unknown secret type')
        self.k8s_client(cmd)

    def create_docker_secret(self, docker_config_path):
        self.create_secret(
            "regcred",
            "docker-registry",
            docker_config_path)

    def create_operator_tls_secret(self, certificate_authority_path):
        self.create_secret(
            "couchbase-operator-tls",
            "generic",
            certificate_authority_path)

    def delete_secret(self, secret_name, ignore_errors=True):
        try:
            self.k8s_client("delete secret {}".format(secret_name))
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_secrets(self, secrets):
        for secret in secrets:
            self.delete_secret(secret)

    def create_from_file(self, file_path):
        self.k8s_client("{} -f {}".format('create', file_path))

    def delete_from_file(self, file_path, ignore_errors=True):
        try:
            self.k8s_client("{} -f {}".format('delete', file_path))
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_from_files(self, file_paths):
        for file in file_paths:
            self.delete_from_file(file)

    def delete_cluster(self, ignore_errors=True):
        try:
            self.k8s_client("delete cbc cb-example-perf")
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def describe_cluster(self):
        ret = self.k8s_client('describe cbc', split_lines=False)
        return yaml.safe_load(ret)

    def get_cluster(self):
        raw_cluster = self.k8s_client("get cbc cb-example-perf -o json", split_lines=False)
        cluster = json.loads(raw_cluster.decode('utf8'))
        return cluster

    def get_backups(self):
        raw_backups = self.k8s_client("get couchbasebackups -o json", split_lines=False)
        backups = json.loads(raw_backups.decode('utf8'))
        return backups

    def get_backup(self, backup_name):
        raw_backup = self.k8s_client(
            "get couchbasebackup {} -o json".format(backup_name),
            split_lines=False)
        backup = json.loads(raw_backup.decode('utf8'))
        return backup

    def get_restore(self, restore_name):
        raw_restore = self.k8s_client(
            "get couchbasebackuprestore {} -o json".format(restore_name),
            split_lines=False,
            max_attempts=1
        )
        backup = json.loads(raw_restore.decode('utf8'))
        return backup

    def get_bucket(self):
        raw_cluster = self.k8s_client("get cbc cb-example-perf -o json", split_lines=False)
        cluster = json.loads(raw_cluster.decode('utf8'))
        return cluster

    def get_operator_version(self):
        for pod in self.get_pods():
            name = pod['metadata']['name']
            if 'couchbase-operator' in name and 'admission' not in name:
                containers = pod['spec']['containers']
                for container in containers:
                    if container['name'] == 'couchbase-operator':
                        image = container['image']
                        build = image.split(":")[-1]
                        return build.split("-")[0]
        raise Exception("could not get operator version")

    def delete_all_buckets(self, timeout=1200):
        self.k8s_client('delete couchbasebuckets --all')
        self.wait_for_cluster_ready(timeout=timeout)

    def delete_all_pvc(self):
        self.k8s_client('delete pvc --all')

    def delete_all_backups(self, ignore_errors=True):
        try:
            self.k8s_client('delete couchbasebackups --all')
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_all_pods(self):
        self.k8s_client('delete pods --all')

    def wait_for(self, condition_func, condition_params=None, timeout=1200):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if condition_params:
                if condition_func(condition_params=condition_params):
                    return
            else:
                if condition_func():
                    return
            time.sleep(1)
        raise Exception('timeout: condition not reached')

    def wait_for_cluster_ready(self, timeout=1200):
        self.wait_for(self.cluster_ready, timeout=timeout)

    def cluster_ready(self):
        cluster = self.get_cluster()
        status = cluster.get('status', {})
        conditions = status.get('conditions', {})
        if not conditions:
            return False

        for condition in conditions:
            if condition['type'] == 'Available':
                if condition['status'] != 'True':
                    return False
            elif condition['type'] == 'Balanced':
                if condition['status'] != 'True':
                    return False
            else:
                logger.info(
                    'condition type unknown: {} : {}'.format(condition['type'],
                                                             condition['status']))
                return False
        return True

    def wait_for_pods_ready(self, pod, desired_num, namespace="default", timeout=1200):
        self.wait_for(self.pods_ready,
                      condition_params=(pod, desired_num, namespace),
                      timeout=timeout)

    def pods_ready(self, condition_params):
        pod = condition_params[0]
        desired_num = condition_params[1]
        namespace = condition_params[2]
        pods = self.get_pods(namespace=namespace)
        num_rdy = 0
        for check_pod in pods:
            check_pod_name = check_pod.get("metadata", {}).get("name", "")
            if pod in check_pod_name and check_pod_name.count("-") == pod.count("-") + 2:
                check_pod_status = check_pod["status"]
                initialized = False
                ready = False
                containers_ready = False
                pod_scheduled = False
                for condition in check_pod_status.get("conditions", []):
                    if condition["status"] == "True":
                        if condition["type"] == "Initialized":
                            initialized = True
                        elif condition["type"] == "Ready":
                            ready = True
                        elif condition["type"] == "ContainersReady":
                            containers_ready = True
                        elif condition["type"] == "PodScheduled":
                            pod_scheduled = True
                if pod_scheduled and initialized and containers_ready and ready:
                    num_rdy += 1
        return num_rdy == desired_num

    def wait_for_admission_controller_ready(self):
        self.wait_for_pods_ready("couchbase-operator-admission", 1)

    def wait_for_operator_ready(self):
        self.wait_for_pods_ready("couchbase-operator", 1)

    def wait_for_couchbase_pods_ready(self, node_count):
        self.wait_for_pods_ready("cb-example", node_count)

    def wait_for_rabbitmq_operator_ready(self):
        self.wait_for_pods_ready("rabbitmq-cluster-operator", 1, "rabbitmq-system")

    def wait_for_rabbitmq_broker_ready(self):
        self.wait_for_pods_ready("rabbitmq-rabbitmq", 1)

    def wait_for_pods_deleted(self, pod, namespace="default", timeout=1200):
        self.wait_for(self.pods_deleted,
                      condition_params=(pod, namespace),
                      timeout=timeout)

    def wait_for_operator_deletion(self):
        self.wait_for_pods_deleted('cb-example')
        self.wait_for_pods_deleted('couchbase-operator-admission')
        self.wait_for_pods_deleted('couchbase-operator')

    def wait_for_rabbitmq_deletion(self):
        self.wait_for_pods_deleted('rabbitmq-rabbitmq-server')
        self.wait_for_pods_deleted("rabbitmq-cluster-operator", "rabbitmq-system")

    def wait_for_workers_deletion(self):
        self.wait_for_pods_deleted("worker")

    def pods_deleted(self, condition_params):
        pod = condition_params[0]
        namespace = condition_params[1]
        pods = self.get_pods(namespace=namespace)
        for check_pod in pods:
            check_pod_name = check_pod.get("metadata", {}).get("name", "")
            if pod in check_pod_name and check_pod_name.count("-") == pod.count("-") + 2:
                return False
        return True

    def wait_for_svc_deployment(
        self, svc_name: str, namespace: str = "default", timeout: int = 120
    ):
        """Wait until a service is listed by k8s or a timeout is reached."""
        self.wait_for(self.service_up, condition_params=(svc_name, namespace), timeout=timeout)

    def service_up(self, condition_params: tuple[str, str]) -> bool:
        """Check if service is listed by k8s."""
        svc_name, namespace = condition_params
        for svc in self.get_services(namespace):
            if svc.get("metadata", {}).get("name") == svc_name:
                return True
        return False

    def get_broker_urls(self):
        ret = self.k8s_client(
            "get secret rabbitmq-rabbitmq-default-user -o jsonpath='{.data.username}'")
        b64_username = ret[0].decode("utf-8")
        username = base64.b64decode(b64_username).decode("utf-8")
        ret = self.k8s_client(
            "get secret rabbitmq-rabbitmq-default-user -o jsonpath='{.data.password}'")
        b64_password = ret[0].decode("utf-8")
        password = base64.b64decode(b64_password).decode("utf-8")
        ret = self.k8s_client("get pods -o wide")
        for line in ret:
            line = line.decode("utf-8")
            if "rabbitmq-rabbitmq-server" in line:
                node = line.split()[6]
        ret = self.k8s_client("get nodes -o wide")
        for line in ret:
            line = line.decode("utf-8")
            if node in line:
                ip = line.split()[6]
        ret = self.k8s_client("get svc -o wide")
        for line in ret:
            line = line.decode("utf-8")
            if "rabbitmq-rabbitmq-client" in line:
                ports = line.split()[4].split(",")
                ports = [port.split("/")[0] for port in ports]
                ports = [port.split(":") for port in ports]
                for port in ports:
                    if port[0] == '5672':
                        mapped_port = port[1]
                    if port[0] == '15672':
                        ui_port = port[1]
        broker_ui_url = "amqp://{}:{}@{}:{}".format(username, password, ip, ui_port)
        broker_url = "amqp://{}:{}@{}:{}/broker".format(username, password, ip, mapped_port)
        return broker_url, broker_ui_url

    def upload_rabbitmq_config(self):
        rabbitmq_config_path = "cloud/broker/rabbitmq/0.48/definitions.json"
        raw_url = self.get_broker_urls()[1]
        username_password = raw_url.split("//")[1].split("@")[0]
        url = raw_url.split("//")[1].split("@")[1]
        with open(rabbitmq_config_path) as data:
            requests.post('http://{}/api/definitions'.format(url),
                          headers={'Content-Type': 'application/json'},
                          data=data,
                          auth=(username_password.split(":")[0],
                                username_password.split(":")[1]))

    def start_memcached(self, mem_limit: int = 20000, port: int = 11211):
        # Deploy memcached and its headless service
        logger.info(f"Starting memcached with mem_limit: {mem_limit} on port {port}")
        self.create_from_file("cloud/operator/memcached.yaml")
        self.wait_for_pods_ready("memcached", 1)
        self.wait_for_svc_deployment("memcached-service")

    def clone_ycsb(self, repo: str, branch: str, worker_home: str, ycsb_instances: int):
        repo = repo.replace("git://", "https://")
        self.init_ycsb(repo, branch, worker_home, None)
        workers = self.get_worker_pods()
        for worker in workers:
            for instance in range(1, ycsb_instances + 1):
                self.kubectl_exec(worker, f"cp -r YCSB YCSB_{instance}")

    def init_ycsb(self, repo: str, branch: str, worker_home: str, sdk_version: None):
        ret = self.k8s_client("get pods")
        for line in ret:
            line = line.decode("utf-8")
            if "worker" in line:
                worker_name = line.split()[0]
                self.kubectl_exec(worker_name, 'rm -rf YCSB')
                self.kubectl_exec(worker_name, 'git clone -q -b {} {}'.format(branch, repo))
                if sdk_version is not None:
                    sdk_version = sdk_version.replace(":", ".")
                    major_version = sdk_version.split(".")[0]
                    cb_version = "couchbase"
                    if major_version == "1":
                        cb_version += ""
                    else:
                        cb_version += major_version
                    original_string = '<{0}.version>*.*.*<\\/{0}.version>'.format(cb_version)
                    new_string = '<{0}.version>{1}<\\/{0}.version>'.format(cb_version, sdk_version)
                    cmd = "sed -i 's/{}/{}/g' pom.xml".format(original_string, new_string)
                    self.kubectl_exec(worker_name, 'cd YCSB; {}'.format(cmd))

    def build_ycsb(self, worker_home: str, ycsb_client: str):
        ret = self.k8s_client("get pods")
        for line in ret:
            line = line.decode("utf-8")
            if "worker" in line:
                worker_name = line.split()[0]
                cmd = f"pyenv local 2.7.18 && bin/ycsb build {ycsb_client}"

                logger.info('Running: {}'.format(cmd))
                self.kubectl_exec(worker_name, 'cd YCSB; {}'.format(cmd))

    def sanitize_meta(self, config):
        config['generation'] = 0
        config['resourceVersion'] = ''
        config.pop('creationTimestamp', None)
        return config

    def dump_config_to_yaml_file(self, config, path):
        with open(path, 'w+') as file:
            yaml.dump(config, file)

    def get_celery_logs(self, worker_home: str):
        logger.info('Collecting remote Celery logs')
        for worker in self.get_worker_pods():
            cmd = "cp default/{0}:worker_{0}.log celery/worker_{0}.log" \
                .format(worker)
            self.k8s_client(cmd)

    def get_export_files(self, worker_home: str):
        logger.info('Collecting YCSB export files')
        for worker in self.get_worker_pods():
            self.get_worker_ycsb_files(worker, "ycsb*.log")

    def get_worker_ycsb_files(self, worker: str, pattern: str):
        lines = self.kubectl_exec(worker, f"ls YCSB*/{pattern}")
        for line in lines:
            for member in line.decode("utf-8").split():
                filename = member.split("/")[-1]
                cmd = f"cp default/{worker}:{member} YCSB/{filename}"
                self.k8s_client(cmd)

    def get_syncgateway_ycsb_logs(self, worker_home: str, sgs: SyncgatewaySettings, local_dir: str):
        pattern = f"{sgs.log_title}*"
        logger.info("Collecting YCSB logs")
        for worker in self.get_worker_pods():
            self.get_worker_ycsb_files(worker, pattern)

    def create_backup(self):
        # 2020-12-04T23:33:57Z
        current_utc = datetime.datetime.utcnow()
        minute = (current_utc.minute + 5) % 60
        with CAOCouchbaseBackupFile() as backup_config:
            backup_config.set_schedule_time(f"{minute} * * * *")
            backup_path = backup_config.dest_file
        self.create_from_file(backup_path)

    def wait_for_backup_complete(self, timeout=7200):
        self.wait_for(self.backup_complete, timeout=timeout)

    def backup_complete(self):
        backup = self.get_backup('my-backup')
        status = backup.get('status', None)
        if status:
            failed = status.get('failed', None)
            if failed:
                raise Exception('backup failed')
            running = status.get('running', None)
            last_run = status.get('lastRun', None)
            last_success = status.get('lastSuccess', None)
            duration = status.get('duration', None)
            capacity_used = status.get('capacityUsed', None)
            if last_run and last_success and duration and capacity_used and not running:
                return True
        return False

    def create_restore(self):
        self.create_from_file("cloud/operator/restore.yaml")

    def create_horizontal_pod_autoscaler(
        self,
        cluster_name: str,
        server_group: str,
        min_nodes: int,
        max_nodes: int,
        target_metric: str,
        target_type: str,
        target_value: str,
    ):
        with CAOHorizontalAutoscalerFile() as autoscaler_config:
            autoscaler_config.setup_pod_autoscaler(
                cluster_name,
                server_group,
                min_nodes,
                max_nodes,
                target_metric,
                target_type,
                target_value,
            )
            autoscaler_path = autoscaler_config.dest_file

        self.create_from_file(autoscaler_path)

    def get_logs(self, pod_name: str, container: str = None, options: str = '') -> str:
        # Get from all pod's containers if none is specified,
        # useful for our usecase here
        if not container:
            container = '--all-containers=true'
        else:
            container = '-c {}'.format(container)
        return self.k8s_client("logs {} {} {}".format(pod_name, container, options),
                               split_lines=False).decode('utf8')

    def collect_k8s_logs(self):
        # Collect operator and backup pods logs if one exists
        pods = self.get_pods()
        for pod in pods:
            pod_name = pod.get("metadata", {}).get("name", "")
            # Collect logs for operator, backup and server pods (server, metrics & CNG)
            pod_prefixes = ("couchbase-operator-", "my-backup-", "cb-example-perf-", "sync-gateway")
            if pod_name.startswith(pod_prefixes) and 'admission' not in pod_name:
                logger.info("Collecting pod '{}' logs".format(pod_name))
                logs = self.get_logs(pod_name)
                with open("{}.log".format(pod_name), 'w') as file:
                    file.write(logs)

    def istioctl(self, params, kube_config=None, split_lines=True, max_attempts=1):
        kube_config = kube_config or self.kube_config_path
        params = params.split()
        params = ['istioctl', '--kubeconfig', kube_config] + params

        attempt = 1
        ex = Exception("max attempts exceeded")
        while attempt <= max_attempts:
            if attempt > 1:
                time.sleep(1)
            try:
                res = subprocess.run(params,
                                     check=True,
                                     stderr=subprocess.STDOUT,
                                     stdout=subprocess.PIPE).stdout
                if split_lines:
                    return res.splitlines()
                else:
                    return res
            except Exception as ex_new:
                ex = ex_new
            finally:
                attempt += 1
        raise ex

    def get_ip_port_mapping(self):
        host_to_ip = dict()
        port_translation = dict()
        pods = self.get_pods()
        nodes = self.get_nodes()
        svcs = self.get_services()

        for node_dict in nodes:
            node_name = node_dict['metadata']['name']
            for addr in node_dict['status']['addresses']:
                if addr['type'] == "ExternalIP":
                    host_to_ip[node_name] = addr['address']

        for pod in pods:
            pod_name = pod['metadata']['name']
            if "cb-example-perf" in pod_name:
                pod_node = pod['spec']['nodeName']
                pod_node_ip = host_to_ip[pod_node]
                pod_host_name = "{}.cb-example-perf.default.svc".format(pod_name)
                host_to_ip[pod_host_name] = pod_node_ip
                host_to_ip[pod_name] = pod_node_ip
                host_to_ip[pod_node_ip] = pod_node_ip
                for svc_dict in svcs:
                    if svc_dict['metadata']['name'] == pod_name:
                        forwarded_ports = {
                            str(port_dict['targetPort']): str(port_dict['nodePort'])
                            for port_dict in svc_dict['spec']['ports']
                        }
                        ports = {
                            str(port_dict['nodePort']): str(port_dict['nodePort'])
                            for port_dict in svc_dict['spec']['ports']
                        }

                        port_translation[pod_node_ip] = dict(ports, **forwarded_ports)
                        break
            elif "sync-gateway" in pod_name:
                pod_node = pod["spec"]["nodeName"]
                pod_node_ip = host_to_ip[pod_node]
                host_to_ip[pod_name] = pod_node_ip
                host_to_ip[pod_node_ip] = pod_node_ip
                for svc_dict in svcs:
                    if svc_dict["metadata"]["name"] == "syncgateway-service":
                        forwarded_ports = {
                            str(port_dict["targetPort"]): str(port_dict["nodePort"])
                            for port_dict in svc_dict["spec"]["ports"]
                        }
                        ports = {
                            str(port_dict["nodePort"]): str(port_dict["nodePort"])
                            for port_dict in svc_dict["spec"]["ports"]
                        }
                        port_translation[pod_node_ip] = dict(ports, **forwarded_ports)
                        break
        return host_to_ip, port_translation

    def start_celery_worker(self,
                            worker_name,
                            worker_hostname,
                            broker_url):
        cmd = 'C_FORCE_ROOT=1 ' \
              'PYTHONOPTIMIZE=1 ' \
              'PYTHONWARNINGS=ignore ' \
              'WORKER_TYPE=remote ' \
              'BROKER_URL={1} ' \
              'nohup env/bin/celery -A perfrunner.helpers.worker worker' \
              ' -l INFO -Q {0} -n {0} --discard &>worker_{2}.log &' \
            .format(worker_hostname,
                    broker_url,
                    worker_name)
        self.kubectl_exec(worker_name, cmd)

    def terminate_client_pods(self, worker_path):
        try:
            self.delete_from_file(worker_path)
            self.wait_for_pods_deleted("worker")
        except Exception as ex:
            logger.info(ex)

    def pull_perfrunner_patch(self, cherrypick: str = None):
        pull_cmd = "git pull origin master"
        if cherrypick:
            pull_cmd = f"{pull_cmd} && {cherrypick}"
        workers = self.get_worker_pods()
        for worker in workers:
            self.kubectl_exec(
                worker,
                pull_cmd,
            )
            self.kubectl_exec(worker, "make docker-cloud-worker")

    def detect_core_dumps(self):
        return {}

    def enable_cpu(self):
        pass

    def collect_info(self):
        pass

    def reset_memory_settings(self, host_string: str):
        pass
