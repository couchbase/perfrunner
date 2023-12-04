import os
from argparse import ArgumentParser, Namespace
from collections import namedtuple
from functools import cached_property
from multiprocessing import Process, set_start_method
from typing import Iterator, Optional

import paramiko
import requests
import validators
from fabric.api import cd, run
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.misc import url_exist
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import master_client
from perfrunner.settings import ClusterSpec, TestConfig

set_start_method("fork")

LOCATIONS = (
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/trinity/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/elixir/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/morpheus/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/neo/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/magma-preview/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/cheshire-cat/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/mad-hatter/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/alice/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/vulcan/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/spock/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/watson/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/master/{build}/',
    'http://172.23.126.166/builds/releases/{release}/',
    'http://172.23.126.166/builds/releases/{release}/ce/',
)

PKG_PATTERNS = {
    'rpm': (
        'couchbase-server-{edition}-{release}-{build}-linux.{arch}.rpm',
        'couchbase-server-{edition}-{release}-{build}-{os_name}{os_version}.{arch}.rpm',
        'couchbase-server-{edition}-{release}-linux.{arch}.rpm',
        'couchbase-server-{edition}-{release}-{os_name}{os_version}.{arch}.rpm',
    ),
    'deb': (
        'couchbase-server-{edition}_{release}-{build}-linux_{arch}.deb',
        'couchbase-server-{edition}_{release}-{build}-{os_name}{os_version}_{arch}.deb',
        'couchbase-server-{edition}_{release}-linux_{arch}.deb',
        'couchbase-server-{edition}_{release}-{os_name}{os_version}_{arch}.deb',
    ),
    'exe': (
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.msi',
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.exe',
        'couchbase-server-{edition}_{release}-windows_amd64.exe',
        'couchbase-server-{edition}_{release}-windows_amd64.msi',
    ),
}

ARM_ARCHS = {
    'rpm': 'aarch64',
    'deb': 'arm64'
}

Build = namedtuple('Build', ['filename', 'url'])


def download_file(url: str, filename: str):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def upload_file(file: str, to_host: str, to_user: str, to_password: str, to_directory: str = "."):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(to_host, username=to_user, password=to_password)
    sftp = client.open_sftp()
    sftp.put(file, "{}/{}".format(to_directory, file))
    sftp.close()


class OperatorInstaller:

    def __init__(self, cluster_spec: ClusterSpec, options):
        self.options = options
        self.cluster_spec = cluster_spec
        self.registry_base = "ghcr.io/cb-vanilla/"
        if cluster_spec.cloud_provider == 'openshift':
            self.registry_base = "ghcr.io/cb-rhcc/"

        self.operator_version = self.options.operator_version
        if "-" in self.operator_version:
            self.operator_release = self.operator_version.split("-")[0]
            self.operator_tag = '{}operator:{}' \
                .format(self.registry_base, self.operator_version)
            self.admission_controller_release = self.operator_version.split("-")[0]
            self.admission_controller_tag = '{}admission-controller:{}' \
                .format(self.registry_base, self.operator_version)
        else:
            self.operator_release = self.operator_version
            self.operator_tag = 'couchbase/operator:{}' \
                .format(self.operator_version)
            self.admission_controller_release = self.operator_version
            self.admission_controller_tag = 'couchbase/admission-controller:{}' \
                .format(self.operator_version)

        self.couchbase_version = self.options.couchbase_version
        if "-" in self.couchbase_version:
            self.couchbase_release = self.couchbase_version.split("-")[0]
            self.couchbase_tag = '{}server:{}' \
                .format(self.registry_base, self.couchbase_version)
        else:
            self.couchbase_release = self.couchbase_version
            self.couchbase_tag = 'couchbase/server:{}' \
                .format(self.couchbase_version)

        self.operator_backup_version = self.options.operator_backup_version
        if self.operator_backup_version:
            if "-" in self.operator_backup_version:
                self.operator_backup_release = self.operator_backup_version.split("-")[0]
                self.operator_backup_tag = '{}operator-backup:{}' \
                    .format(self.registry_base, self.operator_backup_version)
            else:
                self.operator_backup_release = self.operator_backup_version
                self.operator_backup_tag = 'couchbase/operator-backup/{}' \
                    .format(self.operator_backup_version)
        else:
            self.operator_backup_tag = '{}operator-backup:latest' \
                .format(self.registry_base)

        self.exporter_version = self.options.exporter_version or self.cluster_spec \
            .infrastructure_settings.get('exporter_version', '1.0.7')  # For now default to 1.0.7
        if "-" in self.exporter_version:
            self.exporter_release = self.exporter_version.split("-")[0]
            self.exporter_tag = '{}exporter:{}' \
                .format(self.registry_base, self.exporter_version)
        else:
            self.exporter_release = self.exporter_version
            self.exporter_tag = 'couchbase/exporter:{}' \
                .format(self.exporter_version)

        self.node_count = len(self.cluster_spec.infrastructure_clusters['couchbase1'].split())
        self.refresh_rate = self.cluster_spec.infrastructure_settings.get('refresh_rate', '60')

        self.remote = RemoteHelper(cluster_spec)

        self.docker_config_path = os.path.expanduser("~") + "/.docker/config.json"
        self.operator_base_path = "cloud/operator/{}/{}" \
            .format(self.operator_release.split(".")[0],
                    self.operator_release.split(".")[1])
        self.certificate_authority_path = "{}/ca.crt" \
            .format(self.operator_base_path)
        self.crd_path = "{}/crd.yaml" \
            .format(self.operator_base_path)
        self.config_path = "{}/config.yaml" \
            .format(self.operator_base_path)
        self.config_template_path = "{}/config_template.yaml" \
            .format(self.operator_base_path)
        self.auth_path = "{}/auth_secret.yaml" \
            .format(self.operator_base_path)
        self.cb_cluster_path = "{}/couchbase-cluster.yaml" \
            .format(self.operator_base_path)
        self.template_cb_cluster_path = "{}/couchbase-cluster_template.yaml" \
            .format(self.operator_base_path)
        self.worker_base_path = "cloud/worker"
        self.worker_path = "{}/worker.yaml" \
            .format(self.worker_base_path)
        self.rmq_base_path = "cloud/broker/rabbitmq/0.48"
        self.rmq_operator_path = "{}/cluster-operator.yaml" \
            .format(self.rmq_base_path)
        self.rmq_cluster_path = "{}/rabbitmq.yaml" \
            .format(self.rmq_base_path)

    def install(self):
        self.install_operator()
        self.install_celery_broker()

    def install_operator(self):
        logger.info("installing operator")
        self.create_secrets()
        self.create_crd()
        self.create_config()
        self.wait_for_operator_and_admission()
        self.create_auth()
        # At this stage, just prepare the cb cluster config, but dont deploy anything
        self.create_cluster_config()

    def install_celery_broker(self):
        logger.info("installing celery broker")
        self.create_rabbitmq_operator()
        self.wait_for_rabbitmq_operator()
        self.create_rabbitmq_cluster()
        self.wait_for_rabbitmq_cluster()
        self.creating_rabbitmq_config()

    def uninstall(self):
        self.uninstall_operator()
        self.uninstall_celery_broker()
        self.uninstall_workers()
        self.delete_artifacts()

    def uninstall_operator(self):
        logger.info("uninstalling operator")
        self.delete_operator_files()
        self.delete_operator_secrets()
        self.wait_for_operator_deletion()

    def uninstall_celery_broker(self):
        logger.info("uninstalling celery broker")
        self.delete_rabbitmq_files()
        self.wait_for_rabbitmq_deletion()

    def uninstall_workers(self):
        logger.info("uninstall workers")
        self.delete_worker_files()
        self.wait_for_worker_deletion()

    def create_secrets(self):
        logger.info("creating secrets")
        self.remote.create_docker_secret(
            self.docker_config_path)
        self.remote.create_operator_tls_secret(
            self.certificate_authority_path)

    def create_crd(self):
        logger.info("creating CRD")
        self.remote.create_from_file(self.crd_path)

    def create_config(self):
        logger.info("creating config")
        self.remote.create_operator_config(
            self.config_template_path,
            self.config_path,
            self.operator_tag,
            self.admission_controller_tag)

    def create_auth(self):
        logger.info("creating auth")
        self.remote.create_from_file(self.auth_path)

    def create_cluster_config(self):
        logger.info("preparing couchbase cluster config")
        self.remote.create_couchbase_cluster_config(
            self.template_cb_cluster_path,
            self.cb_cluster_path,
            self.couchbase_tag,
            self.operator_backup_tag,
            self.exporter_tag,
            self.node_count,
            self.refresh_rate)

    def wait_for_operator_and_admission(self):
        logger.info("waiting for operator and admission controller")
        self.remote.wait_for_admission_controller_ready()
        self.remote.wait_for_operator_ready()

    def create_rabbitmq_operator(self):
        logger.info("creating rabbitmq operator")
        self.remote.create_from_file(self.rmq_operator_path)

    def wait_for_rabbitmq_operator(self):
        logger.info("waiting for rabbitmq operator")
        self.remote.wait_for_rabbitmq_operator_ready()

    def create_rabbitmq_cluster(self):
        logger.info("creating rabbitmq cluster")
        self.remote.create_from_file(self.rmq_cluster_path)

    def wait_for_rabbitmq_cluster(self):
        logger.info("waiting for rabbitmq cluster")
        self.remote.wait_for_rabbitmq_broker_ready()

    def creating_rabbitmq_config(self):
        logger.info("creating rabbitmq config")
        self.remote.upload_rabbitmq_config()

    def delete_operator_files(self):
        logger.info("deleting operator files")
        files = [self.cb_cluster_path, self.auth_path,
                 self.config_path, self.crd_path]
        self.remote.delete_from_files(files)

    def delete_operator_secrets(self):
        logger.info("deleting operator secrets")
        secrets = ['regcred', 'couchbase-operator-tls',
                   'couchbase-server-tls', 'user-password-secret']
        self.remote.delete_secrets(secrets)

    def wait_for_operator_deletion(self):
        logger.info("waiting for operator deletion")
        self.remote.wait_for_operator_deletion()

    def delete_rabbitmq_files(self):
        logger.info("deleting rabbit mq files")
        self.remote.delete_from_files(
            [self.rmq_cluster_path,
             self.rmq_operator_path])

    def wait_for_rabbitmq_deletion(self):
        logger.info("waiting for rabbitmq deletion")
        self.remote.wait_for_rabbitmq_deletion()

    def delete_worker_files(self):
        logger.info("deleting worker files")
        self.remote.delete_from_file(self.worker_path)

    def wait_for_worker_deletion(self):
        logger.info("waiting for worker deletion")
        self.remote.wait_for_workers_deletion()

    def delete_artifacts(self):
        logger.info("deleting any artifact pods, pvcs, and backups")
        self.remote.delete_all_backups()
        self.remote.delete_all_pods()
        self.remote.delete_all_pvc()


class KubernetesInstaller:

    def __init__(self, cluster_spec, options):
        self.options = options
        self.cluster_spec = cluster_spec
        self.operator_installer = OperatorInstaller(cluster_spec, options)

    def install(self):
        self.install_storage_class()
        self.install_istio()
        self.operator_installer.install()

    def uninstall(self):
        self.operator_installer.uninstall()
        self.uninstall_istio()
        self.uninstall_storage_class()

    def install_storage_class(self):
        raise NotImplementedError

    def uninstall_storage_class(self):
        raise NotImplementedError

    def install_istio(self):
        raise NotImplementedError

    def uninstall_istio(self):
        raise NotImplementedError


class EKSInstaller(KubernetesInstaller):
    STORAGE_CLASSES = {
        'default': None,
        'gp2': 'cloud/infrastructure/aws/eks/ebs-gp2-sc.yaml'
    }

    def __init__(self, cluster_spec, options):
        super().__init__(cluster_spec, options)

    def install_storage_class(self):
        scs = self.operator_installer.remote.get_storage_classes()
        for sc in scs['items']:
            sc_name = sc['metadata']['name']
            self.operator_installer.remote.delete_storage_class(sc_name)
        for cluster in self.cluster_spec.kubernetes_clusters():
            sc = self.cluster_spec.kubernetes_storage_class(cluster)
            if self.STORAGE_CLASSES[sc]:
                self.operator_installer.remote.create_from_file(self.STORAGE_CLASSES[sc])

    def uninstall_storage_class(self):
        for cluster in self.cluster_spec.kubernetes_clusters():
            sc = self.cluster_spec.kubernetes_storage_class(cluster)
            if self.STORAGE_CLASSES[sc]:
                self.operator_installer.remote.delete_from_file(self.STORAGE_CLASSES[sc])

    def install_istio(self):
        if not self.cluster_spec.istio_enabled('k8s_cluster_1'):
            return
        istio_install_cmd = "install " \
                            "--set profile=default " \
                            "--set values.global.defaultNodeSelector.'NodeRoles'=utilities -y"
        istio_label_cmd = "label namespace default istio-injection=enabled --overwrite"
        self.operator_installer.remote.istioctl(istio_install_cmd)
        self.operator_installer.remote.k8s_client(istio_label_cmd)

    def uninstall_istio(self):
        if not self.cluster_spec.istio_enabled('k8s_cluster_1'):
            return
        self.operator_installer.remote.istioctl("x uninstall --purge -y")
        self.operator_installer.remote.delete_namespace("istio-system")
        self.operator_installer.remote.k8s_client("label namespace default istio-injection-")

    def install_kubernetes_dashboard(self):
        pass

    def uninstall_kubernetes_dashboard(self):
        pass


class AKSInstaller(KubernetesInstaller):

    def __init__(self, cluster_spec, options):
        super().__init__(cluster_spec, options)


class GKEInstaller(KubernetesInstaller):

    def __init__(self, cluster_spec, options):
        super().__init__(cluster_spec, options)


class CouchbaseInstaller:

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.options = options
        self.cluster_spec = cluster_spec

    @cached_property
    def url(self) -> str:
        if validators.url(self.options.couchbase_version):
            return self.options.couchbase_version

        return self.find_package(edition=self.options.edition)

    @cached_property
    def debuginfo_url(self) -> str:
        debuginfo_str = ''
        if self.url.endswith('.rpm'):
            debuginfo_str = '-debuginfo'
        elif self.url.endswith('.deb'):
            debuginfo_str = '-dbg'

        search_str = 'couchbase-server-{}'.format(self.options.edition)

        return self.url.replace(search_str, search_str + debuginfo_str)

    @property
    def release(self) -> str:
        return self.options.couchbase_version.split('-')[0]

    @property
    def build(self) -> str:
        split = self.options.couchbase_version.split('-')
        if len(split) > 1:
            return split[1]

    def find_package(self, edition: str, package: Optional[str] = None,
                     os_name: Optional[str] = None,
                     os_version: Optional[str] = None) -> Optional[str]:
        for url in self.url_iterator(edition, package, os_name, os_version):
            if self.is_exist(url):
                return url
        logger.interrupt('Target build not found')

    def url_iterator(self, edition: str, package: Optional[str] = None,
                     os_name: Optional[str] = None,
                     os_version: Optional[str] = None) -> Iterator[str]:
        package = package or self.remote.package  # package type based on server OS
        os_name = os_name or self.remote.distro
        os_version = os_version or self.remote.distro_version

        arch_strings = ['x86_64', 'amd64']
        arch = self.cluster_spec.infrastructure_settings.get('os_arch', 'x86_64')
        if arch == 'arm':
            arch_strings = [ARM_ARCHS[package]]

        for pkg_pattern in PKG_PATTERNS[package]:
            for loc_pattern in LOCATIONS:
                for arch_string in arch_strings:
                    url = loc_pattern + pkg_pattern
                    yield url.format(release=self.release, build=self.build, edition=edition,
                                     os_name=os_name, os_version=os_version, arch=arch_string)

    @staticmethod
    def is_exist(url: str) -> bool:
        try:
            status_code = requests.head(url).status_code
        except ConnectionError:
            return False

        return status_code == 200

    def download_local(self, local_copy_url: Optional[str] = None):
        """Download and save a copy of the specified package."""
        try:
            url = local_copy_url or self.url
            logger.info('Saving a local copy of {}'.format(url))
            download_file(url, 'couchbase.{}'.format(self.remote.package))

        except (Exception, BaseException):
            logger.info("Saving local copy for ubuntu failed, package may not present")

    def download_remote(self):
        """Download and save a copy of the specified package on a remote client."""
        if self.remote.package == 'deb':
            logger.info('Saving a remote copy of {}'.format(self.url))
            self.wget(url=self.url)
        else:
            logger.interrupt('Unsupported package format')

    @master_client
    def wget(self, url: str):
        logger.info('Fetching {}'.format(url))
        with cd('/tmp'):
            run('wget -nc "{}"'.format(url))
            package = url.split('/')[-1]
            run('mv {} couchbase.deb'.format(package))

    def kill_processes(self):
        self.remote.kill_processes()

    def uninstall_package(self):
        self.remote.uninstall_couchbase()

    def clean_data(self):
        self.remote.clean_data()

    def install_debuginfo(self):
        if not url_exist(self.debuginfo_url):
            logger.interrupt('Debuginfo package not found for url {}'.format(self.debuginfo_url))

        self.remote.install_cb_debug_package(url=self.debuginfo_url)

    def install_package(self):
        logger.info('Using this URL: {}'.format(self.url))
        self.remote.upload_iss_files(self.release)
        self.remote.install_couchbase(self.url)

    def check_for_serverless(self, serverless_enabled: str):
        if (str(serverless_enabled).lower() == 'true' or
                self.cluster_spec.goldfish_infrastructure):
            logger.info("Enabling Serverless mode")
            self.remote.enable_serverless_mode()
        else:
            logger.info("Disabling Serverless profile")
            self.remote.disable_serverless_mode()

    def install(self):
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.install_package()


class CloudInstaller(CouchbaseInstaller):

    def __init__(self, cluster_spec, options):
        super().__init__(cluster_spec, options)

    def install_package(self):

        def upload_couchbase(to_host, to_user, to_password, package):
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(to_host, username=to_user, password=to_password)
            sftp = client.open_sftp()
            sftp.put(package, "/tmp/{}".format(package))
            sftp.close()

        user, password = self.cluster_spec.ssh_credentials

        self.remote.upload_iss_files(self.release)
        package_name = "couchbase.{}".format(self.remote.package)

        if self.options.remote_copy:
            url = self.url
            if self.options.local_copy_url:
                url = self.options.local_copy_url
                logger.info('Saving a local url {}'.format(url))
            elif 'aarch64' in self.url:
                url = self.url.replace('aarch64', 'x86_64')
                logger.info('Saving a local copy of x86_64 {}'.format(url))
            else:
                logger.info('Saving a local copy of {}'.format(url))

            download_file(url, package_name)

            for client in self.cluster_spec.workers:
                logger.info('uploading client package {} to {} '.format(package_name, client))
                upload_couchbase(client, user, password, package_name)

        logger.info('Server URL: {}'.format(self.url))
        download_file(self.url, package_name)

        if not self.cluster_spec.has_any_capella:
            logger.info('Uploading {} to servers'.format(package_name))
            uploads = []
            hosts = self.cluster_spec.servers

            for host in hosts:
                logger.info('Uploading {} to {}'.format(package_name, host))
                args = (host, user, password, package_name)

                worker_process = Process(target=upload_couchbase, args=args)
                worker_process.daemon = True
                worker_process.start()
                uploads.append(worker_process)

            for process in uploads:
                process.join()

            self.remote.install_uploaded_couchbase(package_name)


class ClientUploader(CouchbaseInstaller):

    @cached_property
    def url(self) -> str:
        if validators.url(self.options.couchbase_version):
            return self.options.couchbase_version

        return self.options.local_copy_url or self.find_package(edition=self.options.edition,
                                                                package='deb',
                                                                os_name='ubuntu',
                                                                os_version='20.04')

    def upload(self, package_name: str):
        logger.info('Using this URL: {}'.format(self.url))
        logger.info('Uploading {} to clients'.format(package_name))
        uploads = []
        user, password = self.cluster_spec.ssh_credentials
        hosts = self.cluster_spec.workers
        for host in hosts:
            logger.info('Uploading {} to {}'.format(package_name, host))
            args = (package_name, host, user, password, "/tmp")

            worker_process = Process(target=upload_file, args=args)
            worker_process.daemon = True
            worker_process.start()
            uploads.append(worker_process)

        for process in uploads:
            process.join()

    def install(self):
        package_name = 'couchbase.rpm' if self.url.endswith('.rpm') else 'couchbase.deb'

        logger.info('Saving a local copy of {}'.format(self.url))
        download_file(self.url, package_name)

        self.upload(package_name)


class KafkaInstaller:

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.options = options
        self.cluster_spec = cluster_spec

    def install(self):
        self.remote.install_kafka(self.options.kafka_version)


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-v', '--version', '--url', '-cv', '--couchbase-version',
                        required=True,
                        dest='couchbase_version',
                        help='the build version or the HTTP URL to a package')
    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a cluster specification file')
    parser.add_argument('--cluster-name', dest='cluster_name',
                        help='if there are multiple clusters in the cluster spec, this lets you '
                             'name just one of them to set up (default: all clusters)')
    parser.add_argument('-e', '--edition',
                        choices=['enterprise', 'community'],
                        default='enterprise',
                        help='the cluster edition')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('--local-copy',
                        action='store_true',
                        help='save a local copy of a package')
    parser.add_argument('--remote-copy',
                        action='store_true',
                        help='save a remote copy of a package')
    parser.add_argument('--local-copy-url',
                        default=None,
                        help='The local copy url of the build')
    parser.add_argument('-ov', '--operator-version',
                        dest='operator_version',
                        help='the build version for the couchbase operator')
    parser.add_argument('--exporter-version',
                        dest='exporter_version',
                        default=None,
                        help='the build version for the couchbase prometheus exporter')
    parser.add_argument('-obv', '--operator-backup-version',
                        dest='operator_backup_version',
                        help='the build version for the couchbase operator')
    parser.add_argument('--kafka-version',
                        default='2.8.2',
                        help='the Kafka version to install on Kafka nodes')
    parser.add_argument('-u', '--uninstall',
                        action='store_true',
                        help='uninstall the installed build')
    parser.add_argument('--serverless-profile',
                        default=False,
                        help='use to convert the cb-server profile to \
                        serverless on non-capella machines')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster settings')
    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(fname=args.cluster, override=args.override)
    if args.cluster_name:
        cluster_spec.set_active_clusters_by_name([args.cluster_name])

    if cluster_spec.cloud_infrastructure:
        if cluster_spec.kubernetes_infrastructure:
            infra_provider = cluster_spec.infrastructure_settings['provider']
            if infra_provider == 'aws' or infra_provider == 'openshift':
                installer = EKSInstaller(cluster_spec, args)
            elif infra_provider == 'azure':
                installer = AKSInstaller(cluster_spec, args)
            elif infra_provider == 'gcp':
                installer = GKEInstaller(cluster_spec, args)
            else:
                raise Exception("{} is not a valid infrastructure provider"
                                .format(infra_provider))
        elif cluster_spec.has_any_capella:
            installer = ClientUploader(cluster_spec, args)
        else:
            installer = CloudInstaller(cluster_spec, args)
            installer.check_for_serverless(args.serverless_profile)

        if args.uninstall:
            installer.uninstall()
        else:
            installer.install()
    else:
        installer = CouchbaseInstaller(cluster_spec, args)
        installer.check_for_serverless(args.serverless_profile)
        installer.install()
        if args.local_copy:
            installer.download_local(args.local_copy_url)
        if args.remote_copy:
            logger.info('Saving a remote copy')
            installer.download_remote()

        # Here we install CB debuginfo
        # It only works if the linux_perf_profile_flag setting is set through an OVERRIDE
        test_config = TestConfig()
        test_config.override(args.override)
        if test_config.profiling_settings.linux_perf_profile_flag:
            installer.install_debuginfo()

    if cluster_spec.infrastructure_kafka_clusters:
        kafka_installer = KafkaInstaller(cluster_spec, args)
        kafka_installer.install()


if __name__ == '__main__':
    main()
