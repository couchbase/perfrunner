import itertools
import os
import re
from argparse import ArgumentParser, Namespace
from collections import namedtuple
from functools import cached_property
from multiprocessing import Process, set_start_method
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urlparse

import paramiko
import requests
import validators
from fabric.api import cd, run

from logger import logger
from perfrunner.helpers.config_files import CAOConfigFile, CAOCouchbaseClusterFile, CAOWorkerFile
from perfrunner.helpers.local import (
    check_if_remote_branch_exists,
    clone_git_repo,
    extract_any,
    run_custom_cmd,
)
from perfrunner.helpers.misc import create_build_tuple, maybe_atoi, pretty_dict, url_exist
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import master_client
from perfrunner.settings import CBProfile, ClusterSpec, TestConfig

set_start_method("fork")

SERVER_LOCATIONS = (
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/trinity/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/neo/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/morpheus/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/elixir/{build}/',
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

COLUMNAR_LOCATIONS = (
    'http://172.23.126.166/builds/latestbuilds/couchbase-columnar/1.0.0/{build}/',
    'http://172.23.126.166/builds/latestbuilds/capella-analytics/1.0.0/{build}/',
)

PKG_PATTERNS = {
    'rpm': (
        'couchbase-{product}-{edition}-{release}-{build}-linux.{arch}.rpm',
        'couchbase-{product}-{edition}-{release}-{build}-{os_name}{os_version}.{arch}.rpm',
        'couchbase-{product}-{edition}-{release}-linux.{arch}.rpm',
        'couchbase-{product}-{edition}-{release}-{os_name}{os_version}.{arch}.rpm',
    ),
    'deb': (
        'couchbase-{product}-{edition}_{release}-{build}-linux_{arch}.deb',
        'couchbase-{product}-{edition}_{release}-{build}-{os_name}{os_version}_{arch}.deb',
        'couchbase-{product}-{edition}_{release}-linux_{arch}.deb',
        'couchbase-{product}-{edition}_{release}-{os_name}{os_version}_{arch}.deb',
    ),
    'exe': (
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.msi',
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.exe',
        'couchbase-server-{edition}_{release}-windows_amd64.exe',
        'couchbase-server-{edition}_{release}-windows_amd64.msi',
    ),
}

OPERATOR_LOCATIONS = {
    "internal": "http://172.23.126.166/builds/latestbuilds/couchbase-operator/{release}/{build}/",
    "release": "http://172.23.126.166/builds/releases/couchbase-operator/{release}/",
}

OPERATOR_PACKAGES = {
    "internal": "couchbase-autonomous-operator_{release}-{build}-kubernetes-linux-amd64.tar.gz",
    "release": "couchbase-autonomous-operator_{release}-kubernetes-linux-amd64.tar.gz",
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
    sftp.put(file, f'{to_directory}/{file}')
    sftp.close()


class OperatorInstaller:
    def __init__(self, cluster_spec: ClusterSpec, options):
        self.cluster_spec = cluster_spec
        self.registry_base = "ghcr.io/cb-vanilla/"
        if cluster_spec.cloud_provider == 'openshift':
            self.registry_base = "ghcr.io/cb-rhcc/"

        self.operator_version = options.operator_version
        # Operator and admission controller image tags
        self.operator_release, self.operator_tag = self._get_image_tag_for(
            "operator", self.operator_version
        )
        _, self.controller_tag = self._get_image_tag_for(
            "admission-controller", self.operator_version
        )
        # CB server image tag
        self.couchbase_version = options.couchbase_version
        self.couchbase_release, self.couchbase_tag = self._get_image_tag_for(
            "server", self.couchbase_version
        )
        # Backup image tag
        self.backup_version = options.operator_backup_version
        _, self.operator_backup_tag = self._get_image_tag_for(
            "operator-backup", self.backup_version
        )
        # Exporter image tag
        self.exporter_version = (
            options.exporter_version or cluster_spec.infrastructure_settings.get("exporter_version")
        )
        _, self.exporter_tag = self._get_image_tag_for("exporter", self.exporter_version)

        self.node_count = len(cluster_spec.infrastructure_clusters["couchbase1"].split())
        self.refresh_rate = int(cluster_spec.infrastructure_settings.get("refresh_rate", "60"))

        self.remote = RemoteHelper(cluster_spec)

        self.docker_config_path = os.path.expanduser("~") + "/.docker/config.json"
        self.crd_path = "cloud/operator/crd.yaml"
        self.auth_path = "cloud/operator/auth_secret.yaml"
        self.cb_cluster_path = "cloud/operator/couchbase-cluster.yaml"
        self.rmq_operator_path = "cloud/broker/rabbitmq/0.48/cluster-operator.yaml"
        self.rmq_cluster_path = "cloud/broker/rabbitmq/0.48/rabbitmq.yaml"

    def _get_image_tag_for(self, component: str, version_string: str) -> tuple[str, str]:
        if not version_string:
            return None, None

        if "-" in version_string:
            image_tag = f"{self.registry_base}{component}:{version_string}"
        else:
            image_tag = f"couchbase/{component}:{version_string}"

        release = version_string.split("-")[0]
        return release, image_tag

    def install(self):
        self.install_operator()
        self.install_celery_broker()

    def prepare_operator_files(self):
        source = "internal" if "-" in self.operator_version else "release"
        build = self.operator_version.split("-")[-1]
        package_name = OPERATOR_PACKAGES.get(source).format(
            release=self.operator_release, build=build
        )
        operator_url = (
            OPERATOR_LOCATIONS.get(source).format(release=self.operator_release, build=build)
            + package_name
        )
        if url_exist(operator_url):
            download_file(operator_url, package_name)
            extract_any(package_name, "couchbase-operator")
            source_dir = "couchbase-operator"
        else:
            # Cases where the specified build doesnt exist, generate the CRD from source
            self.make_operator(self.operator_release)
            source_dir = "couchbase-operator/example"
        # Copy generated CRD file to the operator directory
        run_custom_cmd("./", "cp", f"{source_dir}/crd.yaml cloud/operator/")

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
        self.remote.create_docker_secret(self.docker_config_path)

    def create_crd(self):
        logger.info("creating CRD")
        self.prepare_operator_files()
        self.remote.create_from_file(self.crd_path)

    def create_config(self):
        logger.info("creating config")
        config_file = None
        with CAOConfigFile(self.operator_release, self.operator_tag, self.controller_tag) as config:
            config.setup_config()
            config_file = config.dest_file

        self.remote.create_from_file(config_file)

    def create_auth(self):
        logger.info("creating auth")
        self.remote.create_from_file(self.auth_path)

    def create_cluster_config(self):
        logger.info("preparing couchbase cluster config")
        with CAOCouchbaseClusterFile(self.couchbase_release, self.cluster_spec) as cb_config:
            cb_config.set_server_spec(self.couchbase_tag, self.node_count)
            cb_config.set_backup(self.operator_backup_tag)
            cb_config.set_exporter(self.exporter_tag, self.refresh_rate)

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

    def make_operator(self, release: str):
        """Generate operator CRD from source."""
        github_token = os.getenv("GITHUB_ACCESS_TOKEN", "")
        repo = f"https://{github_token}@github.com/couchbase/couchbase-operator.git"
        # Operator branches are in the form of <major>.<minor>.x ex 2.6.x
        branch = f"{release.rsplit('.', 1)[0]}.x"
        # If a version branch doesnt exist, use master
        branch = branch if check_if_remote_branch_exists(repo, branch) else "master"
        clone_git_repo(repo, branch)
        run_custom_cmd("couchbase-operator", "make", "crd")

    def delete_operator_files(self):
        logger.info("deleting operator files")
        files = [
            self.cb_cluster_path,
            self.auth_path,
            CAOConfigFile(self.operator_release, self.operator_tag, self.controller_tag).dest_file,
            self.crd_path,
        ]
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
        self.remote.delete_from_file(CAOWorkerFile(self.cluster_spec).dest_file)

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
        if validators.url(url := self.options.couchbase_version):
            logger.info('Checking if provided package URL is valid.')
            if url_exist(url):
                return url
            logger.interrupt(f'Invalid URL: {url}')

        return self.find_package(edition=self.options.edition)

    @cached_property
    def debuginfo_url(self) -> str:
        debuginfo_str = ''
        if self.url.endswith('.rpm'):
            debuginfo_str = '-debuginfo'
        elif self.url.endswith('.deb'):
            debuginfo_str = '-dbg'

        return re.sub(
            rf'couchbase-(server|columnar)-{self.options.edition}',
            rf'couchbase-\1-{self.options.edition}{debuginfo_str}',
            self.url
        )

    @property
    def release(self) -> str:
        return self.options.couchbase_version.split('-')[0]

    @property
    def build(self) -> Optional[str]:
        _, *build = self.options.couchbase_version.split('-')
        return build[0] if build else None

    @property
    def build_tuple(self) -> tuple:
        if validators.url(self.options.couchbase_version):
            return ()

        return create_build_tuple(self.options.couchbase_version)

    def find_package(self, edition: str, package: Optional[str] = None,
                     os_name: Optional[str] = None, os_version: Optional[str] = None,
                     arch: Optional[str] = None) -> Optional[str]:
        package = package or self.remote.package  # package type based on server OS
        os_name = os_name or self.remote.distro
        os_version = os_version or self.remote.distro_version
        arch = arch or self.cluster_spec.infrastructure_settings.get('os_arch', 'x86_64')

        for url in self.url_iterator(edition, package, os_name, os_version, arch):
            if url_exist(url):
                return url

        logger.interrupt(
            'Package URL not found for following criteria:\n' +
            pretty_dict({
                'edition': edition,
                'release': self.release,
                'build': self.build,
                'os_name': os_name,
                'os_version': os_version,
                'arch': arch,
                'package': package
            }, sort_keys=False)
        )

    def url_iterator(self, edition: str, package: Optional[str] = None,
                     os_name: Optional[str] = None, os_version: Optional[str] = None,
                     arch: Optional[str] = None) -> Iterator[str]:
        arch_strings = ['x86_64', 'amd64']
        if arch == 'arm':
            arch_strings = [ARM_ARCHS[package]]

        products = ['server']
        locations = SERVER_LOCATIONS
        if self.cluster_spec.goldfish_infrastructure:
            products.insert(0, 'columnar')
            locations = COLUMNAR_LOCATIONS + locations

        for loc_pattern, product, pkg_pattern, arch_str in itertools.product(
            locations, products, PKG_PATTERNS[package], arch_strings
        ):
            url = loc_pattern + pkg_pattern
            yield url.format(
                product=product,
                edition=edition,
                release=self.release,
                build=self.build,
                os_name=os_name,
                os_version=os_version,
                arch=arch_str
            )

    def download_local(self, local_copy_url: Optional[str] = None):
        """Download and save a copy of the specified package."""
        try:
            url = local_copy_url or self.url
            logger.info(f'Saving a local copy of {url}')
            download_file(url, f'couchbase.{self.remote.package}')

        except (Exception, BaseException):
            logger.info("Saving local copy for ubuntu failed, package may not present")

    def download_remote(self):
        """Download and save a copy of the specified package on a remote client."""
        if self.remote.package == 'deb':
            logger.info(f'Saving a remote copy of {self.url}')
            self.wget(url=self.url)
        else:
            logger.interrupt('Unsupported package format')

    @master_client
    def wget(self, url: str):
        logger.info(f'Fetching {url}')
        with cd('/tmp'):
            run(f'wget -nc "{url}"')
            package = url.split('/')[-1]
            run(f'mv {package} couchbase.deb')

    def kill_processes(self):
        self.remote.kill_processes()

    def uninstall_package(self):
        self.remote.uninstall_couchbase()

    def clean_data(self):
        self.remote.clean_data()

    def install_debuginfo(self):
        if not url_exist(self.debuginfo_url):
            logger.interrupt(f'Debuginfo package not found for url {self.debuginfo_url}')

        self.remote.install_cb_debug_package(url=self.debuginfo_url)

    def install_package(self):
        logger.info(f'Using this URL: {self.url}')
        self.remote.upload_iss_files(self.release)
        self.remote.download_and_install_couchbase(self.url)

    def set_cb_profile(self):
        profile = CBProfile.DEFAULT
        if self.options.serverless_profile:
            profile = CBProfile.SERVERLESS
        elif self.options.columnar_profile:
            profile = CBProfile.COLUMNAR
        elif self.cluster_spec.goldfish_infrastructure:
            if (
                (7, 6, 100) <= self.build_tuple < (8, 0, 0) or
                Path(urlparse(self.url).path).name.startswith('couchbase-columnar')
            ):
                profile = CBProfile.COLUMNAR
            else:
                profile = CBProfile.SERVERLESS

        self.remote.set_cb_profile(profile)

    def install(self):
        logger.info('Finding package to use...')
        logger.info(f'Package URL: {self.url}')
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.set_cb_profile()
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
            sftp.put(package, f'/tmp/{package}')
            sftp.close()

        user, password = self.cluster_spec.ssh_credentials

        self.remote.upload_iss_files(self.release)
        package_name = f'couchbase.{self.remote.package}'

        if self.options.remote_copy:
            url = self.url
            if self.options.local_copy_url:
                url = self.options.local_copy_url
                logger.info(f'Saving a local url {url}')
            elif 'aarch64' in self.url:
                url = self.url.replace('aarch64', 'x86_64')
                logger.info(f'Saving a local copy of x86_64 {url}')
            else:
                logger.info(f'Saving a local copy of {url}')

            download_file(url, package_name)

            for client in self.cluster_spec.workers:
                logger.info(f'uploading client package {package_name} to {client}')
                upload_couchbase(client, user, password, package_name)

        logger.info(f'Server URL: {self.url}')
        download_file(self.url, package_name)

        if not self.cluster_spec.capella_infrastructure:
            logger.info(f'Uploading {package_name} to servers')
            uploads = []
            hosts = self.cluster_spec.servers

            for host in hosts:
                logger.info(f'Uploading {package_name} to {host}')
                args = (host, user, password, package_name)

                worker_process = Process(target=upload_couchbase, args=args)
                worker_process.daemon = True
                worker_process.start()
                uploads.append(worker_process)

            for process in uploads:
                process.join()

            self.remote.install_uploaded_couchbase(package_name)
            self.remote.start_server()


class ClientUploader(CouchbaseInstaller):

    @cached_property
    def url(self) -> str:
        for url in (self.options.couchbase_version, self.options.local_copy_url):
            if validators.url(url):
                logger.info('Checking if provided package URL is valid.')
                if url_exist(url):
                    return url
                logger.interrupt(f'Invalid URL: {url}')

        return self.find_package(edition=self.options.edition,
                                 package='deb',
                                 os_name='ubuntu',
                                 os_version='20.04')

    def upload(self, package_name: str):
        logger.info(f'Using this URL: {self.url}')
        logger.info(f'Uploading {package_name} to clients')
        uploads = []
        user, password = self.cluster_spec.ssh_credentials
        hosts = self.cluster_spec.workers
        for host in hosts:
            logger.info(f'Uploading {package_name} to {host}')
            args = (package_name, host, user, password, "/tmp")

            worker_process = Process(target=upload_file, args=args)
            worker_process.daemon = True
            worker_process.start()
            uploads.append(worker_process)

        for process in uploads:
            process.join()

    def install(self):
        package_name = 'couchbase.rpm' if self.url.endswith('.rpm') else 'couchbase.deb'

        logger.info(f'Saving a local copy of {self.url}')
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
                        type=maybe_atoi,
                        default=False,
                        help='use to convert the cb-server profile to \
                        serverless on non-capella machines')
    parser.add_argument('--columnar-profile',
                        type=maybe_atoi,
                        default=False,
                        help='use to convert the cb-server profile to \
                        columnar on non-capella machines')
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
                raise Exception(f'{infra_provider} is not a valid infrastructure provider')
        elif cluster_spec.capella_infrastructure:
            installer = ClientUploader(cluster_spec, args)
        else:
            installer = CloudInstaller(cluster_spec, args)

        if args.uninstall:
            installer.uninstall()
        else:
            installer.install()
    else:
        installer = CouchbaseInstaller(cluster_spec, args)
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
