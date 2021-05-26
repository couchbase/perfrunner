import base64
import datetime
import json
import subprocess
import time

import requests
import yaml

import perfrunner.helpers.misc as misc
from logger import logger
from perfrunner.remote import Remote


class RemoteKubernetes(Remote):

    def __init__(self, cluster_spec, os):
        super().__init__(cluster_spec, os)
        self.kube_config_path = "cloud/infrastructure/generated/kube_configs/k8s_cluster_1"
        self.base_path = 'cloud/operator'
        self.cluster_file = 'couchbase-cluster.yaml'
        self.bucket_template_file = 'bucket_template.yaml'
        self.backup_template_file = 'backup_template.yaml'
        self.backup_file = 'backup.yaml'
        self.restore_template_file = 'restore_template.yaml'
        self.restore_file = 'restore.yaml'
        self.autoscaler_template_file = 'autoscaler_template.yaml'
        self.autoscaler_file = 'autoscaler.yaml'
        self.operator_version = None

    def kubectl(self, params, kube_config=None, split_lines=True, max_attempts=3):
        if not kube_config:
            kube_config = self.kube_config_path
        params = params.split()
        if params[0] == 'exec':
            params = params[0:5] + [" ".join(params[5::])]
        params = ['kubectl', '--kubeconfig', kube_config] + params

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

    def kubectl_exec(self, pod, params):
        return self.kubectl("exec {} -- bash -c {}".format(pod, params),
                            kube_config=self.kube_config_path)

    def create_namespace(self, name):
        self.kubectl(
            "create namespace {}".format(name),
            kube_config=self.kube_config_path
        )

    def delete_namespace(self, name):
        self.kubectl(
            "delete namespace {}".format(name),
            kube_config=self.kube_config_path
        )

    def get_pods(self, namespace="default"):
        raw_pods = self.kubectl(
            "get pods -o json -n {}".format(namespace),
            kube_config=self.kube_config_path,
            split_lines=False
        )
        pods = json.loads(raw_pods.decode('utf8'))
        return pods["items"]

    def get_services(self, namespace="default"):
        raw_svcs = self.kubectl(
            "get svc -o json -n {}".format(namespace),
            kube_config=self.kube_config_path,
            split_lines=False
        )
        svcs = json.loads(raw_svcs.decode('utf8'))
        return svcs["items"]

    def get_nodes(self):
        raw_nodes = self.kubectl(
            "get nodes -o json",
            kube_config=self.kube_config_path,
            split_lines=False
        )
        nodes = json.loads(raw_nodes.decode('utf8'))
        return nodes["items"]

    def get_storage_classes(self):
        raw_sc = self.kubectl(
            "get sc -o json",
            kube_config=self.kube_config_path,
            split_lines=False
        )
        sc = json.loads(raw_sc.decode('utf8'))
        return sc

    def get_jobs(self):
        raw_jobs = self.kubectl(
            "get jobs -o json",
            kube_config=self.kube_config_path,
            split_lines=False
        )
        jobs = json.loads(raw_jobs.decode('utf8'))
        return jobs

    def get_cronjobs(self):
        raw_cronjobs = self.kubectl(
            "get cronjobs -o json",
            kube_config=self.kube_config_path,
            split_lines=False
        )
        cronjobs = json.loads(raw_cronjobs.decode('utf8'))
        return cronjobs

    def delete_storage_class(self, storage_class, ignore_errors=True):
        try:
            self.kubectl("delete sc {}".format(storage_class),
                         kube_config=self.kube_config_path)
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
        else:
            raise Exception('unknown secret type')
        self.kubectl(cmd, kube_config=self.kube_config_path)

    def create_docker_secret(self, docker_config_path):
        self.create_secret(
            "regcred",
            "docker",
            docker_config_path)

    def create_operator_tls_secret(self, certificate_authority_path):
        self.create_secret(
            "couchbase-operator-tls",
            "generic",
            certificate_authority_path)

    def create_operator_config(self,
                               config_template_path,
                               config_path,
                               operator_tag,
                               admission_controller_tag):
        misc.copy_template(config_template_path,
                           config_path)
        misc.inject_config_tags(config_path,
                                operator_tag,
                                admission_controller_tag)
        self.create_from_file(config_path)

    def create_couchbase_cluster(self,
                                 template_cb_cluster_path,
                                 cb_cluster_path,
                                 couchbase_tag,
                                 operator_tag,
                                 node_count):
        misc.copy_template(template_cb_cluster_path,
                           cb_cluster_path)
        misc.inject_cluster_tags(cb_cluster_path,
                                 couchbase_tag,
                                 operator_tag)
        misc.inject_server_count(cb_cluster_path,
                                 node_count)
        self.create_from_file(cb_cluster_path)

    def delete_secret(self, secret_name, ignore_errors=True):
        try:
            self.kubectl("delete secret {}".format(secret_name),
                         kube_config=self.kube_config_path)
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_secrets(self, secrets):
        for secret in secrets:
            self.delete_secret(secret)

    def create_from_file(self, file_path):
        self.kubectl("{} -f {}".format('create', file_path),
                     kube_config=self.kube_config_path)

    def delete_from_file(self, file_path, ignore_errors=True):
        try:
            self.kubectl("{} -f {}".format('delete', file_path),
                         kube_config=self.kube_config_path)
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_from_files(self, file_paths):
        for file in file_paths:
            self.delete_from_file(file)

    def delete_cluster(self, ignore_errors=True):
        try:
            self.kubectl("delete cbc cb-example-perf",
                         kube_config=self.kube_config_path)
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def create_cluster(self):
        cluster_path = self.get_cluster_path()
        self.create_from_file(cluster_path)

    def describe_cluster(self):
        ret = self.kubectl('describe cbc',
                           kube_config=self.kube_config_path,
                           split_lines=False)
        return yaml.safe_load(ret)

    def get_cluster(self):
        raw_cluster = self.kubectl("get cbc cb-example-perf -o json",
                                   kube_config=self.kube_config_path,
                                   split_lines=False)
        cluster = json.loads(raw_cluster.decode('utf8'))
        return cluster

    def get_backups(self):
        raw_backups = self.kubectl("get couchbasebackups -o json",
                                   kube_config=self.kube_config_path,
                                   split_lines=False)
        backups = json.loads(raw_backups.decode('utf8'))
        return backups

    def get_backup(self, backup_name):
        raw_backup = self.kubectl(
            "get couchbasebackup {} -o json".format(backup_name),
            kube_config=self.kube_config_path,
            split_lines=False)
        backup = json.loads(raw_backup.decode('utf8'))
        return backup

    def get_restore(self, restore_name):
        raw_restore = self.kubectl(
            "get couchbasebackuprestore {} -o json".format(restore_name),
            kube_config=self.kube_config_path,
            split_lines=False,
            max_attempts=1
        )
        backup = json.loads(raw_restore.decode('utf8'))
        return backup

    def get_bucket(self):
        raw_cluster = self.kubectl("get cbc cb-example-perf -o json",
                                   kube_config=self.kube_config_path,
                                   split_lines=False)
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

    def get_cluster_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(
            self.base_path,
            self.operator_version.split(".")[0],
            self.operator_version.split(".")[1],
            self.cluster_file)

    def get_bucket_path(self, bucket_name):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}.yaml".format(
            self.base_path,
            self.operator_version.split(".")[0],
            self.operator_version.split(".")[1],
            bucket_name)

    def get_bucket_template_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.bucket_template_file)

    def update_cluster_config(self, cluster, timeout=1200, reboot=False):
        cluster_path = self.get_cluster_path()
        if reboot:
            self.delete_cluster()
            self.wait_for_pods_deleted('cb-example', timeout=timeout)
            cluster['metadata'] = {'name': 'cb-example-perf'}
            cluster.pop('status', None)
            self.dump_config_to_yaml_file(cluster, cluster_path)
            self.create_cluster()
            self.wait_for_cluster_ready(timeout=timeout)
        else:
            cluster['metadata'] = self.sanitize_meta(cluster['metadata'])
            self.dump_config_to_yaml_file(cluster, cluster_path)
            self.kubectl(
                'replace -f {}'.format(cluster_path),
                kube_config=self.kube_config_path)
            self.wait_for_cluster_ready(timeout=timeout)

    def update_bucket_config(self, bucket, timeout=1200):
        cluster_path = self.get_cluster_path()
        bucket['metadata'] = self.sanitize_meta(bucket['metadata'])
        bucket_path = "cloud/operator/2/1/{}.yaml".format(bucket['metadata']['name'])
        self.dump_config_to_yaml_file(bucket, bucket_path)
        self.kubectl('replace -f {}'.format(cluster_path),
                     kube_config=self.kube_config_path)
        self.wait_for_cluster_ready(timeout=timeout)

    def create_bucket(self, bucket_name, mem_quota, bucket_config, timeout=30):
        bucket_template_path = self.get_bucket_template_path()
        bucket_path = self.get_bucket_path(bucket_name)
        misc.copy_template(bucket_template_path, bucket_path)
        with open(bucket_path, 'r') as file:
            bucket = yaml.load(file, Loader=yaml.FullLoader)
        bucket['metadata']['name'] = bucket_name
        bucket['spec'] = {
            'memoryQuota': '{}Mi'.format(mem_quota),
            'replicas': bucket_config.replica_number,
            'evictionPolicy': bucket_config.eviction_policy,
            'compressionMode': str(bucket_config.compression_mode)
            if bucket_config.compression_mode else "off",
            'conflictResolution': str(bucket_config.conflict_resolution_type)
            if bucket_config.conflict_resolution_type else "seqno",
            'enableFlush': True,
            'enableIndexReplica': False,
            'ioPriority': 'high',
        }
        bucket['metadata'] = self.sanitize_meta(bucket['metadata'])
        self.dump_config_to_yaml_file(bucket, bucket_path)
        self.kubectl('create -f {}'.format(bucket_path),
                     kube_config=self.kube_config_path)
        self.wait_for_cluster_ready(timeout=timeout)

    def delete_all_buckets(self, timeout=1200):
        self.kubectl('delete couchbasebuckets --all',
                     kube_config=self.kube_config_path)
        self.wait_for_cluster_ready(timeout=timeout)

    def delete_all_pvc(self):
        self.kubectl('delete pvc --all', kube_config=self.kube_config_path)

    def delete_all_backups(self, ignore_errors=True):
        try:
            self.kubectl('delete couchbasebackups --all', kube_config=self.kube_config_path)
        except Exception as ex:
            if not ignore_errors:
                raise ex

    def delete_all_pods(self):
        self.kubectl('delete pods --all', kube_config=self.kube_config_path)

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

    def get_broker_urls(self):
        ret = self.kubectl(
            "get secret rabbitmq-rabbitmq-default-user -o jsonpath='{.data.username}'",
            kube_config=self.kube_config_path)
        b64_username = ret[0].decode("utf-8")
        username = base64.b64decode(b64_username).decode("utf-8")
        ret = self.kubectl(
            "get secret rabbitmq-rabbitmq-default-user -o jsonpath='{.data.password}'",
            kube_config=self.kube_config_path)
        b64_password = ret[0].decode("utf-8")
        password = base64.b64decode(b64_password).decode("utf-8")
        ret = self.kubectl("get pods -o wide",
                           kube_config=self.kube_config_path)
        for line in ret:
            line = line.decode("utf-8")
            if "rabbitmq-rabbitmq-server" in line:
                node = line.split()[6]
        ret = self.kubectl("get nodes -o wide",
                           kube_config=self.kube_config_path)
        for line in ret:
            line = line.decode("utf-8")
            if node in line:
                ip = line.split()[6]
        ret = self.kubectl("get svc -o wide",
                           kube_config=self.kube_config_path)
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

    def init_ycsb(self, repo: str, branch: str, worker_home: str, sdk_version: None):
        ret = self.kubectl("get pods", kube_config=self.kube_config_path)
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
                    if major_version is "1":
                        cb_version += ""
                    else:
                        cb_version += major_version
                    original_string = '<{0}.version>*.*.*<\/{0}.version>'.format(cb_version)
                    new_string = '<{0}.version>{1}<\/{0}.version>'.format(cb_version, sdk_version)
                    cmd = "sed -i 's/{}/{}/g' pom.xml".format(original_string, new_string)
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
            self.kubectl(cmd, kube_config=self.kube_config_path)

    def get_export_files(self, worker_home: str):
        logger.info('Collecting YCSB export files')
        for worker in self.get_worker_pods():
            lines = self.kubectl_exec(worker, "ls YCSB")
            for line in lines:
                for member in line.decode("utf-8").split():
                    if 'ycsb' in member and '.log' in member:
                        cmd = "cp default/{0}:YCSB/{1} YCSB/{1}" \
                            .format(worker, member)
                        self.kubectl(cmd, kube_config=self.kube_config_path)

    def yaml_to_json(self, file_path):
        with open(file_path, 'r') as file:
            json_from_yaml = yaml.load(file, Loader=yaml.FullLoader)
        return json_from_yaml

    def get_backup_template_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.backup_template_file)

    def get_backup_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.backup_file)

    def create_backup(self):
        backup_template_path = self.get_backup_template_path()
        backup_path = self.get_backup_path()
        misc.copy_template(backup_template_path, backup_path)
        # 2020-12-04T23:33:57Z
        current_utc = datetime.datetime.utcnow()
        minute = current_utc.minute + 5
        cron_schedule = '{} * * * *'.format(minute)
        backup_def = self.yaml_to_json(backup_path)
        backup_def['spec']['full']['schedule'] = cron_schedule
        self.dump_config_to_yaml_file(backup_def, backup_path)
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

    def get_restore_template_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.restore_template_file)

    def get_restore_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.restore_file)

    def create_restore(self):
        restore_template_path = self.get_restore_template_path()
        self.create_from_file(restore_template_path)

    def get_autoscaler_template_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.autoscaler_template_file)

    def get_autoscaler_path(self):
        if not self.operator_version:
            self.operator_version = self.get_operator_version()
        return "{}/{}/{}/{}".format(self.base_path,
                                    self.operator_version.split(".")[0],
                                    self.operator_version.split(".")[1],
                                    self.autoscaler_file)

    def create_horizontal_pod_autoscaler(self, server_group, min_nodes, max_nodes,
                                         target_metric, target_type, target_value):
        autoscaler_template_path = self.get_autoscaler_template_path()
        autoscaler_path = self.get_autoscaler_path()
        misc.copy_template(autoscaler_template_path, autoscaler_path)
        with open(autoscaler_path, 'r') as file:
            autoscaler = yaml.load(file, Loader=yaml.FullLoader)

        cluster = self.get_cluster()
        cluster_name = cluster['metadata']['name']
        autoscaler['spec']['scaleTargetRef']['name'] = "{}.{}".format(server_group, cluster_name)
        autoscaler['spec']['minReplicas'] = min_nodes
        autoscaler['spec']['maxReplicas'] = max_nodes
        autoscaler['spec']['metrics'] = \
            [
                {
                    'type': 'Pods',
                    'pods':
                        {
                            'metric': {'name': target_metric},
                            'target':
                                {
                                    'type': target_type,
                                    target_type: target_value
                                }
                        }
                }
            ]

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
              'nohup env/bin/celery worker -A perfrunner.helpers.worker' \
              ' -l INFO -Q {0} -n {0} -C --discard &>worker_{2}.log &' \
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

    def detect_core_dumps(self):
        return {}

    def enable_cpu(self):
        pass

    def collect_info(self):
        pass

    def reset_memory_settings(self, host_string: str):
        pass
