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
from perfrunner.helpers.config_files import CAOCouchbaseClusterFile, CAOWorkerFile
from perfrunner.helpers.local import (
    cao_generate_config,
    check_if_remote_branch_exists,
    clone_git_repo,
    create_x509_certificates,
    extract_any,
    run_custom_cmd,
)
from perfrunner.helpers.misc import create_build_tuple, pretty_dict, url_exist
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import master_client
from perfrunner.settings import CBProfile, ClusterSpec, TestConfig

try:
    set_start_method("fork")
except RuntimeError:
    pass

LATESTBUILDS_BASE_URL = "http://latestbuilds.service.couchbase.com/builds"

SERVER_CODENAMES = {
    "couchbase-server": (
        "totoro",
        "morpheus",
        "cypher",
        "trinity",
        "neo",
        "elixir",
        "magma-preview",
        "cheshire-cat",
        "mad-hatter",
        "alice",
        "vulcan",
        "spock",
        "watson",
        "master",
    )
}
SERVER_INTERNAL_LOCATIONS = tuple(
    f"{LATESTBUILDS_BASE_URL}/latestbuilds/{product}/{codename}/{{build}}/"
    for product, codenames in SERVER_CODENAMES.items()
    for codename in codenames
)

SERVER_RELEASE_LOCATIONS = (
    f"{LATESTBUILDS_BASE_URL}/releases/{{release}}/",
    f"{LATESTBUILDS_BASE_URL}/releases/{{release}}/ce/",
)

COLUMNAR_CODENAMES = {
    "enterprise-analytics": ("lumina", "phoenix"),
    "couchbase-columnar": ("doric", "ionic", "goldfish", "1.0.0"),
}
COLUMNAR_LOCATIONS = (
    *(
        f"{LATESTBUILDS_BASE_URL}/latestbuilds/{product}/{codename}/{{build}}/"
        for product, codenames in COLUMNAR_CODENAMES.items()
        for codename in codenames
    ),
)

PKG_PATTERNS = {
    "rpm": (
        "{product}-{edition}-{release}-{build}-linux.{arch}.rpm",
        "{product}-{edition}-{release}-{build}-{os_name}{os_version}.{arch}.rpm",
        "{product}-{edition}-{release}-linux.{arch}.rpm",
        "{product}-{edition}-{release}-{os_name}{os_version}.{arch}.rpm",
        "{product}-{release}-{build}-linux.{arch}.rpm",
    ),
    "deb": (
        "{product}-{edition}_{release}-{build}-linux_{arch}.deb",
        "{product}-{edition}_{release}-{build}-{os_name}{os_version}_{arch}.deb",
        "{product}-{edition}_{release}-linux_{arch}.deb",
        "{product}-{edition}_{release}-{os_name}{os_version}_{arch}.deb",
        "{product}_{release}-{build}-linux_{arch}.deb",
    ),
    "exe": (
        "{product}-{edition}_{release}-{build}-windows_amd64.msi",
        "{product}-{edition}_{release}-{build}-windows_amd64.exe",
        "{product}-{edition}_{release}-windows_amd64.exe",
        "{product}-{edition}_{release}-windows_amd64.msi",
    ),
}

OPERATOR_LOCATIONS = {
    "internal": f"{LATESTBUILDS_BASE_URL}/latestbuilds/couchbase-operator/{{release}}/{{build}}/",
    "release": f"{LATESTBUILDS_BASE_URL}/releases/couchbase-operator/{{release}}/",
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
    logger.info(f"Downloading {url} to {filename}")
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
        if cluster_spec.is_openshift:
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

        # CNG image tag
        _, self.cng_tag = self._get_image_tag_for("cloud-native-gateway", options.cng_version)

        self.refresh_rate = int(cluster_spec.infrastructure_settings.get("refresh_rate", "60"))

        self.remote = RemoteHelper(cluster_spec)

        self.docker_config_path = os.path.expanduser("~") + "/.docker/config.json"
        self.crd_path = "cloud/operator/crd.yaml"
        self.auth_path = "cloud/operator/auth_secret.yaml"
        self.rmq_operator_path = "cloud/broker/rabbitmq/0.48/cluster-operator.yaml"
        self.rmq_cluster_path = "cloud/broker/rabbitmq/0.48/rabbitmq.yaml"
        self.config_dir = "cloud/operator/configs/"

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
        if not self.cluster_spec.external_client:
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
        for cluster_name in self.cluster_spec.clusters_modified_names:
            self.create_cluster_config(cluster_name)

    def install_celery_broker(self):
        logger.info("installing celery broker")
        self.create_rabbitmq_operator()
        self.wait_for_rabbitmq_operator()
        self.create_rabbitmq_cluster()
        self.wait_for_rabbitmq_cluster()
        self.creating_rabbitmq_config()

    def uninstall(self):
        self.uninstall_operator()
        if not self.cluster_spec.external_client:
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
        cluster_names = self.cluster_spec.clusters_modified_names

        # In the future we may need to only add available addresses to the certificate.
        # For now add all of our possible adresses
        addresses = {
            "localhost",
            "cbperfoc.com",
            "*.cbperfoc.com",
            "elb.us-east-1.amazonaws.com",
            "host.elb.us-east-1.amazonaws.com",
            "*.elb.us-east-1.amazonaws.com",
            "*.compute-1.amazonaws.com",
        }

        # Add cluster-specific addresses for each cluster. Use shared certificates for all clusters.
        for cluster_name in cluster_names:
            addresses.update(
                {
                    f"{cluster_name}.cbperfoc.com",
                    f"*.{cluster_name}.cbperfoc.com",
                    f"*.{cluster_name}",
                    f"*.{cluster_name}.default",
                    f"*.{cluster_name}.default.svc",
                    f"*.{cluster_name}.default.svc.cluster.local",
                    f"{cluster_name}-srv",
                    f"{cluster_name}-srv.default",
                    f"{cluster_name}-srv.default.svc",
                    f"*.{cluster_name}-srv.default.svc.cluster.local",
                    f"{cluster_name}-cloud-native-gateway-service",
                    f"{cluster_name}-cloud-native-gateway-service.default",
                    f"{cluster_name}-cloud-native-gateway-service.default.svc",
                    f"*.{cluster_name}-cloud-native-gateway-service.default.svc.cluster.local",
                }
            )
        create_x509_certificates(addresses)
        self.remote.create_certificate_secrets()
        self.remote.create_docker_secret(self.docker_config_path)

    def create_crd(self):
        logger.info("creating CRD")
        self.prepare_operator_files()
        self.remote.create_from_file(self.crd_path)

    def create_config(self):
        logger.info("creating operator configs")
        cao_generate_config("admission", self.controller_tag, self.config_dir)
        cao_generate_config("operator", self.operator_tag, self.config_dir)
        cao_generate_config("backup", dest_dir=self.config_dir)
        self.remote.create_from_file(self.config_dir)

    def create_auth(self):
        logger.info("creating auth")
        self.remote.create_from_file(self.auth_path)

    def create_cluster_config(self, cluster_name: str):
        logger.info(f"Preparing couchbase cluster config for {cluster_name}")
        with CAOCouchbaseClusterFile(
            self.couchbase_release, self.cluster_spec, cluster_name
        ) as cb_config:
            cb_config.set_cluster_name()
            cb_config.set_server_spec(self.couchbase_tag)
            cb_config.set_backup(self.operator_backup_tag)
            cb_config.set_exporter(self.exporter_tag, self.refresh_rate)
            if self.cng_tag:
                cb_config.set_cng_version(self.cng_tag)

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
        # Add cluster specs first as they should be deleted before the operator files
        files_for_deletion = [
            f"{CAOCouchbaseClusterFile.CLUSTER_DEST_DIR}/{cluster_name}.yaml"
            for cluster_name in self.cluster_spec.clusters_modified_names
        ]
        files_for_deletion.extend([self.auth_path, self.config_dir, self.crd_path])
        self.remote.delete_from_files(files_for_deletion)

    def delete_operator_secrets(self):
        logger.info("deleting operator secrets")
        secrets = [
            "regcred",
            "couchbase-operator-tls",
            "couchbase-server-ca",
            "couchbase-server-tls",
            "user-password-secret",
        ]
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

    def delete_or_edit_storage_class(self, sc_name: str):
        # Deleting provider managed storage classes will trigger storage class operator
        # to recreate them https://access.redhat.com/solutions/5240851. Make them non-default
        if self.cluster_spec.is_openshift:
            self.remote.make_storage_class_non_default(sc_name)
        else:
            self.remote.delete_storage_class(sc_name)


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
            self.operator_installer.delete_or_edit_storage_class(sc_name)
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
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, options: Namespace):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.options = options
        self.cluster_spec = cluster_spec
        self.test_config = test_config

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
            if self.build_tuple >= (8, 0, 0) or self.package_name.startswith(
                "enterprise-analytics"
            ):
                debuginfo_str = "-dbgsym"
            else:
                debuginfo_str = "-dbg"

        product_name = "-".join(self.package_name.split("-")[:2])
        return re.sub(rf"({product_name}-{self.options.edition})", rf"\1{debuginfo_str}", self.url)

    @property
    def package_name(self) -> str:
        return Path(urlparse(self.url).path).name

    @property
    def package_is_columnar(self) -> bool:
        return any(self.package_name.startswith(product) for product in COLUMNAR_CODENAMES)

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

    def find_package(
        self,
        edition: str,
        package_type: Optional[str] = None,
        os_name: Optional[str] = None,
        os_version: Optional[str] = None,
        arch: Optional[str] = None,
    ) -> Optional[str]:
        package_type = package_type or self.remote.package_type  # package type based on server OS
        os_name = os_name or self.remote.distro
        os_version = os_version or self.remote.distro_version
        arch = arch or self.cluster_spec.infrastructure_settings.get('os_arch', 'x86_64')

        for url in self.url_iterator(edition, package_type, os_name, os_version, arch):
            if url_exist(url):
                return url

        logger.interrupt(
            "Package URL not found for following criteria:\n"
            + pretty_dict(
                {
                    "edition": edition,
                    "release": self.release,
                    "build": self.build,
                    "os_name": os_name,
                    "os_version": os_version,
                    "arch": arch,
                    "package_type": package_type,
                },
                sort_keys=False,
            )
        )

    def url_iterator(
        self,
        edition: str,
        package_type: str,
        os_name: Optional[str] = None,
        os_version: Optional[str] = None,
        arch: Optional[str] = None,
    ) -> Iterator[str]:
        arch_strings = ['x86_64', 'amd64']
        if arch == 'arm':
            arch_strings = [ARM_ARCHS[package_type]]

        products = list(SERVER_CODENAMES)
        if self.build is None:
            locations = SERVER_RELEASE_LOCATIONS
            logger.info("No build specified; searching only release packages...")
        elif self.cluster_spec.columnar_infrastructure:
            products = list(COLUMNAR_CODENAMES) + products
            locations = COLUMNAR_LOCATIONS + SERVER_INTERNAL_LOCATIONS
            logger.info(
                "Cluster spec specifies Columnar infrastructure; including columnar "
                "packages in search..."
            )
        else:
            locations = SERVER_INTERNAL_LOCATIONS

        for loc_pattern, product, pkg_pattern, arch_str in itertools.product(
            locations, products, PKG_PATTERNS[package_type], arch_strings
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
            download_file(url, f"couchbase.{self.remote.package_type}")

        except (Exception, BaseException):
            logger.info("Saving local copy for ubuntu failed, package may not present")

    def download_remote(self):
        """Download and save a copy of the specified package on a remote client."""
        if self.remote.package_type == "deb":
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
        self.remote.reset_systemd_service_conf()

    def install_debuginfo(self):
        if not url_exist(self.debuginfo_url):
            logger.interrupt(f'Debuginfo package not found for url {self.debuginfo_url}')

        self.remote.install_cb_debug_package(url=self.debuginfo_url)

    def install_package(self):
        self.remote.upload_iss_files(self.release)
        self.remote.download_and_install_couchbase(self.url)

    def set_cb_profile(self):
        profile = CBProfile.DEFAULT
        if set_profile := self.test_config.cluster.profile:
            # If a test specifies a profile, use that regardless of the install flags
            profile = CBProfile(set_profile)
        elif self.options.cluster_profile:
            profile = CBProfile(self.options.cluster_profile)
        elif self.cluster_spec.columnar_infrastructure:
            profile = CBProfile.COLUMNAR
            if self.package_is_columnar and self.build_tuple >= (1, 2, 0, 1154):
                profile = CBProfile.ANALYTICS

        self.remote.set_cb_profile(profile)

    def set_ns_server_managed_cgroup(self):
        if self.test_config.cluster.cgroup_managed:
            logger.info("Setting provisioned profile cgroup overrides")
            self.remote.enable_resource_management_with_cgroup()

    def install(self):
        logger.info("Finding package to install...")
        logger.info(f'Package URL: {self.url}')
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.set_ns_server_managed_cgroup()
        self.set_cb_profile()
        self.install_package()


class CloudInstaller(CouchbaseInstaller):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, options: Namespace):
        super().__init__(cluster_spec, test_config, options)

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

        if self.options.remote_copy:
            for url in (self.options.local_copy_url, self.options.couchbase_version):
                if url and validators.url(url):
                    logger.info("Checking if provided package URL is valid.")
                    if url_exist(url):
                        client_package_url = url
                        break
                    logger.interrupt(f"Invalid URL: {url}")
            else:
                client_package_url = self.find_package(
                    edition=self.options.edition,
                    package_type="deb",
                    os_name="ubuntu",
                    os_version="20.04",
                    arch="x86_64",
                )

            logger.info(f"Saving a local copy of {client_package_url} to upload to remote clients.")

            client_package_name = f"couchbase.{client_package_url.rsplit('.', 1)[-1]}"
            download_file(client_package_url, client_package_name)

            uploads = []
            for client in self.cluster_spec.workers:
                logger.info(f"uploading client package {client_package_name} to {client}")
                args = (client, user, password, client_package_name)

                worker_process = Process(target=upload_couchbase, daemon=True, args=args)
                worker_process.start()
                uploads.append(worker_process)

            for process in uploads:
                process.join()

        server_package_name = f"couchbase.{self.url.rsplit('.', 1)[-1]}"
        download_file(self.url, server_package_name)

        if not self.cluster_spec.capella_infrastructure:
            logger.info(f"Uploading {server_package_name} to servers")
            uploads = []
            hosts = self.cluster_spec.servers

            for host in hosts:
                logger.info(f"Uploading {server_package_name} to {host}")
                args = (host, user, password, server_package_name)

                worker_process = Process(target=upload_couchbase, args=args)
                worker_process.daemon = True
                worker_process.start()
                uploads.append(worker_process)

            for process in uploads:
                process.join()

            self.remote.install_uploaded_couchbase(server_package_name)
            self.remote.start_server()


class ClientUploader(CouchbaseInstaller):

    @cached_property
    def url(self) -> str:
        for url in (self.options.couchbase_version, self.options.local_copy_url):
            if url and validators.url(url):
                logger.info('Checking if provided package URL is valid.')
                if url_exist(url):
                    return url
                logger.interrupt(f'Invalid URL: {url}')

        return self.find_package(
            edition=self.options.edition, package_type="deb", os_name="ubuntu", os_version="20.04"
        )

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
        if not self.cluster_spec.workers:
            logger.info("There are not clients defined. Nothing to install.")
            return

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

class CBLInstaller:

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.options = options
        self.cluster_spec = cluster_spec
        self.cbl_support_zip_url = self.options.cbl_support_url
        self.cbl_support_dir_name = self.cbl_support_zip_url.split('/')[-1].replace(".zip","")
        self.cbl_support_zip_path = f"/tmp/{self.cbl_support_dir_name}.zip"
        self.cbl_support_dir = f"/tmp/{self.cbl_support_dir_name}"
        self.ld_library_path = f"{self.cbl_support_dir}:{self.cbl_support_dir}/libicu"
        self.cbl_testserver_zip_url = self.options.cbl_url
        self.cbl_testserver_dir_name = self.cbl_testserver_zip_url.split('/')[-1].replace(".zip","")
        self.cbl_testserver_zip_path = f"/tmp/{self.cbl_testserver_dir_name}.zip"
        self.javatestserver_dir = "/opt/javatestserver"

    def install(self):
        logger.info("Uninstalling CBL.")
        self.remote.stop_daemon_manager(self.javatestserver_dir, self.cbl_testserver_dir_name)
        self.remote.remove_line_from_setenv(self.ld_library_path)
        self.remote.uninstall_cbl(self.cbl_support_dir, self.cbl_support_zip_path, \
            self.cbl_testserver_zip_path, self.javatestserver_dir)
        logger.info("Starting CBL Installation.")
        self.remote.download_cbl_support_libs(self.cbl_support_zip_url, self.cbl_support_zip_path)
        self.remote.unzip_cbl_support_libs(self.cbl_support_zip_path, self.cbl_support_dir)
        self.remote.update_setenv(self.ld_library_path)
        self.remote.download_cbl_testserver(self.cbl_testserver_zip_url, \
            self.cbl_testserver_zip_path)
        self.remote.unzip_cbl_testserver(self.cbl_testserver_zip_path, self.javatestserver_dir)
        self.remote.make_executable(self.javatestserver_dir)
        self.remote.run_daemon_manager(
            self.ld_library_path, self.javatestserver_dir, self.cbl_testserver_dir_name
        )


def get_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-v",
        "--version",
        "--url",
        "-cv",
        "--couchbase-version",
        required=True,
        dest="couchbase_version",
        help="the build version or the HTTP URL to a package",
    )
    parser.add_argument(
        "-t",
        "--test",
        dest="test_config_fname",
        default=None,
        help="path to a test configuration file",
    )
    parser.add_argument(
        "-c", "--cluster", required=True, help="the path to a cluster specification file"
    )
    parser.add_argument(
        "--cluster-name",
        dest="cluster_name",
        help="if there are multiple clusters in the cluster spec, this lets you "
        "name just one of them to set up (default: all clusters)",
    )
    parser.add_argument(
        "-e",
        "--edition",
        choices=["enterprise", "community"],
        default="enterprise",
        help="the cluster edition",
    )
    parser.add_argument("--verbose", action="store_true", help="enable verbose logging")
    parser.add_argument("--local-copy", action="store_true", help="save a local copy of a package")
    parser.add_argument(
        "--remote-copy", action="store_true", help="save a remote copy of a package"
    )
    parser.add_argument("--local-copy-url", default=None, help="The local copy url of the build")
    parser.add_argument(
        "-ov",
        "--operator-version",
        dest="operator_version",
        help="the build version for the couchbase operator",
    )
    parser.add_argument(
        "--exporter-version",
        dest="exporter_version",
        default=None,
        help="the build version for the couchbase prometheus exporter",
    )
    parser.add_argument(
        "--cng-version",
        dest="cng_version",
        default=None,
        help="the build version for the couchbase CNG",
    )
    parser.add_argument(
        "-obv",
        "--operator-backup-version",
        dest="operator_backup_version",
        help="the build version for the couchbase operator",
    )
    parser.add_argument(
        "--kafka-version", default="2.8.2", help="the Kafka version to install on Kafka nodes"
    )
    parser.add_argument(
        "-u", "--uninstall", action="store_true", help="uninstall the installed build"
    )
    parser.add_argument(
        "--cluster-profile",
        required=False,
        choices=[p.value for p in CBProfile],
        default=None,
        help="ns_server profile to set for the cluster",
    )
    parser.add_argument(
        "--cbl-url", dest="cbl_url", default=None, help="the HTTP URL to a cbl package"
    )
    parser.add_argument(
        "--cbl-support-url",
        dest="cbl_support_url",
        default=None,
        help="the HTTP URL to a cbl support libraries package",
    )
    parser.add_argument("override", nargs="*", help="custom cluster settings")
    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(fname=args.cluster, override=args.override)
    if args.cluster_name:
        cluster_spec.set_active_clusters_by_name([args.cluster_name])

    test_config = TestConfig()
    if args.test_config_fname:
        test_config.parse(args.test_config_fname, override=args.override)
    else:
        test_config.override(args.override)

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
            installer = ClientUploader(cluster_spec, test_config, args)
        else:
            installer = CloudInstaller(cluster_spec, test_config, args)

        if args.uninstall:
            installer.uninstall()
        else:
            installer.install()
    else:
        installer = CouchbaseInstaller(cluster_spec, test_config, args)
        installer.install()
        if args.local_copy:
            installer.download_local(args.local_copy_url)
        if args.remote_copy:
            logger.info('Saving a remote copy')
            installer.download_remote()

        # Here we install CB debuginfo
        if (
            test_config.profiling_settings.linux_perf_profile_flag
            or test_config.cluster.install_debug_sym
        ):
            installer.install_debuginfo()

    if cluster_spec.infrastructure_kafka_clusters:
        kafka_installer = KafkaInstaller(cluster_spec, args)
        kafka_installer.install()

    if args.cbl_url:
        cbl_installer = CBLInstaller(cluster_spec, args)
        cbl_installer.install()


if __name__ == '__main__':
    main()
