import datetime
import fileinput
import ipaddress
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Optional, Union
from uuid import uuid4

import requests
import validators
import yaml
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, generate_private_key
from cryptography.x509 import (
    AuthorityKeyIdentifier,
    BasicConstraints,
    Certificate,
    CertificateBuilder,
    CertificateSigningRequest,
    CertificateSigningRequestBuilder,
    DirectoryName,
    DNSName,
    ExtendedKeyUsage,
    ExtendedKeyUsageOID,
    GeneralName,
    IPAddress,
    KeyUsage,
    Name,
    NameAttribute,
    SubjectAlternativeName,
    SubjectKeyIdentifier,
    random_serial_number,
)
from cryptography.x509.oid import NameOID

from logger import logger


@dataclass
class SGPortRange:
    min_port: int
    max_port: int = None
    protocol: str = 'tcp'

    def __init__(self, min_port: int, max_port: int = None, protocol: str = 'tcp'):
        self.min_port = min_port
        self.max_port = max_port if max_port else min_port
        self.protocol = protocol

    def port_range_str(self) -> str:
        return '{}{}'.format(
            self.min_port,
            '-{}'.format(self.max_port) if self.max_port != self.min_port else ''
        )

    def __str__(self) -> str:
        return '(ports={}, protocol={})'.format(
            self.port_range_str(),
            self.protocol
        )


def uhex() -> str:
    return uuid4().hex


def pretty_dict(d: Any) -> str:
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def target_hash(*args: str) -> str:
    int_hash = hash(args)
    str_hash = md5(hex(int_hash).encode('utf-8')).hexdigest()
    return str_hash[:6]


def retry(catch: tuple = (), iterations: int = 5, wait: int = 10):
    """Retry a function while discarding the specified exceptions.

    'catch' is a tuple of exceptions. Passing in a list is also fine.

    'iterations' means number of total attempted calls. 'iterations' is only
    meaningful when >= 2.

    'wait' is wait time between calls.

    Usage:

    import perfrunner.helpers.misc

    @perfrunner.helpers.misc.retry(catch=[RuntimeError, KeyError])
    def hi():
        raise KeyError("Key Errrrr from Hi")

    # or if you want to tune your own iterations and wait

    @perfrunner.helpers.misc.retry(
        catch=[KeyError, TypeError],
        iterations=3, wait=1)
    def hi(who):
        print "hi called"
        return "hi " +  who

    print hi("john")
    # this throws TypeError when 'str' and 'None are concatenated
    print hi(None)
    """
    # in case the user specifies a list of Exceptions instead of a tuple
    catch = tuple(catch)

    def retry_decorator(func):
        def retry_wrapper(*arg, **kwargs):
            for i in range(iterations):
                try:
                    result = func(*arg, **kwargs)
                except catch:
                    if i == (iterations - 1):
                        raise
                    else:
                        pass
                else:
                    return result
                time.sleep(wait)
        return retry_wrapper
    return retry_decorator


def read_json(filename: str) -> dict:
    with open(filename) as fh:
        return json.load(fh)


def maybe_atoi(a: str, t=int) -> Union[int, float, str, bool]:
    if a.lower() == 'false':
        return False
    elif a.lower() == 'true':
        return True
    else:
        try:
            return t(a)
        except ValueError:
            return a


def human_format(number: float) -> str:
    magnitude = 0
    while abs(number) >= 1e3:
        magnitude += 1
        number /= 1e3
    return '{:.0f}{}'.format(number, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])


def copy_template(source, dest):
    shutil.copyfile(source, dest)


def url_exist(url: str) -> bool:
    try:
        status_code = requests.head(url).status_code
    except ConnectionError:
        return False

    return status_code == 200


def inject_config_tags(config_path,
                       operator_tag,
                       admission_controller_tag):
    #  operator
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator:build'
        replace = operator_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  admission controller
    with fileinput.FileInput(config_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/admission-controller:build'
        replace = admission_controller_tag
        for line in file:
            print(line.replace(search, replace), end='')


def inject_cluster_tags(cluster_path,
                        couchbase_tag,
                        operator_backup_tag,
                        exporter_tag,
                        refresh_rate):
    #  couchbase
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/server:build'
        replace = couchbase_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  operator backup
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/operator-backup:build'
        replace = operator_backup_tag
        for line in file:
            print(line.replace(search, replace), end='')

    #  exporter
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'couchbase/exporter:build'
        replace = exporter_tag
        for line in file:
            print(line.replace(search, replace), end='')

    # Refresh rate
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'XX'
        replace = refresh_rate
        for line in file:
            print(line.replace(search, replace), end='')


def inject_server_count(cluster_path, server_count):
    with fileinput.FileInput(cluster_path, inplace=True, backup='.bak') as file:
        search = 'size: node_count'
        replace = 'size: {}'.format(server_count)
        for line in file:
            print(line.replace(search, replace), end='')


def inject_workers_spec(num_workers, mem_limit, cpu_limit, worker_template_path, worker_path):
    with open(worker_template_path) as file:
        worker_config = yaml.safe_load(file)

    worker_config['spec']['replicas'] = num_workers
    limits = worker_config['spec']['template']['spec']['containers'][0]['resources']['limits']
    limits['cpu'] = cpu_limit
    limits['memory'] = mem_limit

    with open(worker_path, "w") as file:
        yaml.dump(worker_config, file)


def is_null(element) -> bool:
    if (isinstance(element, int) or isinstance(element, float)) and element == 0:
        return False
    elif isinstance(element, bool):
        return False
    else:
        return False if element else True


def remove_nulls(d: dict) -> dict:
    if not isinstance(d, dict):
        return d
    return {k: new_v for k, v in d.items() if not is_null(new_v := remove_nulls(v))}


def run_local_shell_command(command: str, success_msg: str = '',
                            err_msg: str = '') -> tuple[str, str, int]:
    """Run a shell command locally using `subprocess` and print stdout + stderr on failure.

    Args
    ----
    command: shell command to run
    success_msg: message to print on success
    err_msg: message to print on failure (in addition to command stdout and stderr)

    Returns
    -------
    command stdout, stderr, return code

    """
    process = subprocess.run(command, shell=True, capture_output=True)
    if (returncode := process.returncode) == 0:
        if success_msg:
            logger.info(success_msg)
    else:
        if err_msg:
            logger.error(err_msg)
        logger.error('Command failed with return code {}: {}'
                     .format(returncode, process.args))
        logger.error('Command stdout: {}'.format(process.stdout.decode()))
        logger.error('Command stderr: {}'.format(process.stderr.decode()))

    return process.stdout.decode(), process.stderr.decode(), returncode


def set_azure_subscription(sub_name: str, alias: str) -> int:
    _, _, err = run_local_shell_command(
        command='az account set --subscription "{}"'.format(sub_name),
        success_msg='Set active Azure subscription to "{}" ({})'.format(sub_name, alias),
        err_msg='Failed to set active Azure subscription to "{}" ({})'.format(sub_name, alias)
    )
    return err


def set_azure_perf_subscription() -> int:
    return set_azure_subscription('130 - QE', 'perf')


def set_azure_capella_subscription(capella_env: str) -> int:
    if 'sandbox' in capella_env:
        sub = 'couchbasetest1-rcm'
    else:
        sub = 'capellanonprod-rcm'

    return set_azure_subscription(sub, 'capella')


def get_python_sdk_installation(version: str) -> str:
    """Convert specified version into a format that can be installed by pip.

    Possible version that can be specified in settings:
    1. Full URL/local path to a wheel source
    2. Gerrit reference in the form of refs/changes/X/Y/Z
    3. Commit hash
    4. Version number in the form of x.y.z
    """
    if validators.url(version) or os.path.exists(version):
        # direct url to internal package source or file path
        return '"{}"'.format(version)
    elif 'refs/changes' in version:  # gerrit change
        return 'git+https://review.couchbase.org/couchbase-python-client@{}'.format(version)
    elif '.' not in version:  # git commit
        return 'git+https://github.com/couchbase/couchbase-python-client.git@{}'.format(version)
    else:
        return 'couchbase=={}'.format(version)


def run_aws_cli_command(command_template: str, *args) -> Optional[str]:
    """Run an AWS CLI command formatted with any extra args and return the output (if any)."""
    command = 'env/bin/aws {}'.format(command_template.format(*args))
    logger.info('Running AWS CLI command: {}'.format(command))
    stdout, _, returncode = run_local_shell_command(command)

    output = None
    if returncode == 0:
        output = stdout.strip()
        if output:
            return output

    return output


class SSLCertificate:

    """Generate a self signed CA and node certificate with SAN including cluster nodes IPs.

    Calling `generate()` will create new CA, signed certificate and store them
    in the `INBOX` directory.
    """

    INBOX = 'certificates/inbox/'

    def __init__(self, hosts: list[str] = {'127.0.0.1'}):
        self.hosts = hosts
        self.not_valid_before = datetime.datetime.now(datetime.timezone.utc)
        self.not_valid_after = self.not_valid_before + datetime.timedelta(days=60)

    def generate(self):
        """Generate new CA and a signed certificate and store them and their keys to `INBOX`."""
        logger.info('Generating certificates')
        # Generate CA first
        ca_key, ca = self.generate_ca()

        # Certificate signing request
        request_key, req = self.generate_signing_request(ca)

        # Node certificate
        cert = CertificateBuilder(
            issuer_name=ca.subject,
            subject_name=req.subject,
            public_key=req.public_key(),
            serial_number=random_serial_number(),
            not_valid_before=self.not_valid_before,
            not_valid_after=self.not_valid_after,
            extensions=req.extensions
        ).sign(ca_key, hashes.SHA256())

        # Store key/chain using appropriate names
        self._store_key('pkey', request_key)
        self._store_cert('chain', cert)

    def generate_ca(self) -> tuple[RSAPrivateKey, Certificate]:
        """Generate a certificate that can be used as a Certificate Authority (CA)."""
        ca_key_pair = self._make_key_pair()
        subject = self._get_subject('Couchbase Root CA')
        ca = CertificateBuilder(
            issuer_name=subject,
            subject_name=subject,
            public_key=ca_key_pair.public_key(),
            serial_number=random_serial_number(),
            not_valid_before=self.not_valid_before,
            not_valid_after=self.not_valid_after
        ).add_extension(
            BasicConstraints(ca=True, path_length=None),
            critical=True
        ).add_extension(
            KeyUsage(digital_signature=True, content_commitment=False,
                     key_encipherment=True, data_encipherment=False,
                     key_agreement=False, key_cert_sign=True,
                     crl_sign=False, encipher_only=False, decipher_only=False),
            critical=True
        ).sign(ca_key_pair, hashes.SHA256())

        # Store CA
        self._store_cert('ca', ca)
        self._store_key('ca_key', ca_key_pair)
        return ca_key_pair, ca

    def generate_signing_request(self, ca: Certificate
                                 ) -> tuple[RSAPrivateKey, CertificateSigningRequest]:
        """Generate a CSR and a private key that can be used to generate a certificate."""
        pkey = self._make_key_pair()
        san_list = [self._get_general_name(host) for host in self.hosts]

        req = CertificateSigningRequestBuilder().subject_name(
            self._get_subject('Couchbase Cluster')
        ).add_extension(
            BasicConstraints(ca=False, path_length=None),
            critical=True
        ).add_extension(
            SubjectKeyIdentifier(b'hash'),
            critical=False
        ).add_extension(
            AuthorityKeyIdentifier(None,
                                   authority_cert_issuer=[DirectoryName(ca.issuer)],
                                   authority_cert_serial_number=ca.serial_number),
            critical=False
        ).add_extension(
            KeyUsage(digital_signature=True, content_commitment=False,
                     key_encipherment=True, data_encipherment=False,
                     key_agreement=False, key_cert_sign=False,
                     crl_sign=False, encipher_only=False, decipher_only=False),
            critical=True
        ).add_extension(
            SubjectAlternativeName(san_list),
            critical=False
        ).add_extension(
            ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False
        ).sign(pkey, hashes.SHA256())

        return pkey, req

    def _get_subject(self, common_name: str) -> Name:
        return Name([
            NameAttribute(NameOID.COUNTRY_NAME, 'US'),
            NameAttribute(NameOID.ORGANIZATION_NAME, 'Couchbase'),
            NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, 'Perf Team'),
            NameAttribute(NameOID.COMMON_NAME, common_name),
        ])

    def _make_key_pair(self) -> RSAPrivateKey:
        """Generate an RSA public/private key pair."""
        return generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

    def _store_key(self, key_name: str, key: RSAPrivateKey):
        with open('{}{}.key'.format(self.INBOX, key_name), 'wb') as file:
            file.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))

    def _store_cert(self, cert_name: str, cert: Certificate):
        with open('{}{}.pem'.format(self.INBOX, cert_name), 'wb') as file:
            file.write(cert.public_bytes(serialization.Encoding.PEM))

    def _get_general_name(self, host: str) -> GeneralName:
        # If the host address can pass as an IP v4 or v6 mark it as an IPAddress,
        # DNS name otherwise
        try:
            return IPAddress(ipaddress.ip_address(host))
        except Exception:
            return DNSName(host)
