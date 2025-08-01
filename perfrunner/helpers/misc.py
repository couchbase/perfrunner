import datetime
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from hashlib import md5
from typing import Any, Optional, Union
from uuid import uuid4

import requests
import validators
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


class SafeEnum(Enum):
    """Enum that falls back to the first member if an invalid value is provided."""

    @classmethod
    def _missing_(cls, value):
        fallback = next(iter(cls))
        logger.warning(
            f"Invalid value '{value}' provided for {cls.__name__}. Falling back to {fallback.name}."
        )
        return fallback


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


def pretty_dict(d: Any, sort_keys: bool = True, encoder=lambda o: o.__dict__) -> str:
    return json.dumps(d, indent=4, sort_keys=sort_keys, default=encoder)


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


def human_format(number: float, p: int = 0) -> str:
    p = max(0, p)
    magnitude = 0
    while abs(number) >= 1e3:
        magnitude += 1
        number /= 1e3
    return f'{number:.{p}f}{["", "K", "M", "G", "T", "P"][magnitude]}'


def copy_template(source, dest):
    shutil.copyfile(source, dest)


def url_exist(url: str) -> bool:
    try:
        status_code = requests.head(url).status_code
    except ConnectionError:
        return False

    return status_code == 200


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


def run_local_shell_command(
    command: str,
    success_msg: str = "",
    err_msg: str = "",
    quiet: bool = False,
    env: dict[str, str] = {},
) -> tuple[str, str, int]:
    """Run a shell command locally using `subprocess` and print stdout + stderr on failure.

    Args
    ----
    command: shell command to run
    success_msg: message to print on success
    err_msg: message to print on failure (in addition to command stdout and stderr)
    quiet: if True, don't print stdout and stderr on failure
    env: environment variables to set (adding to the current environment)

    Returns
    -------
    command stdout, stderr, return code

    """
    process = subprocess.run(command, shell=True, capture_output=True, env=os.environ | env)
    if (returncode := process.returncode) == 0:
        if success_msg:
            logger.info(success_msg)
    elif not quiet:
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


def run_aws_cli_command(command_template: str, *args, profile: str = "") -> Optional[str]:
    """Run an AWS CLI command formatted with any extra args.

    If the command fails, return `None`. Otherwise return the stdout.
    """
    profile_flag = f"--profile {profile}" if profile else ""
    command = f"env/bin/aws {profile_flag} {command_template.format(*args)}"
    logger.info(f"Running AWS CLI command: {command}")
    stdout, _, returncode = run_local_shell_command(command)

    if returncode == 0:
        return stdout.strip()

    return None


def parse_prometheus_stat(stats, stat_name: str):
    stat_count = 0
    stat = stats.find(stat_name)
    stat_list = []
    while stat != -1:
        stat_list.append(stat)
        stat = stats.find(stat_name, stat + 1)
    last = stats.find("# HELP", stat_list[-1] + 1)
    stat_list.append(last)
    for i in range(2, len(stat_list) - 1):
        stat_str = stats[stat_list[i]:stat_list[i+1]]
        a = stat_str.find("}")
        stat_count += int(float(stat_str[a+2:]))
    return stat_count


def create_build_tuple(build_str: str) -> tuple[int]:
    """Take a build string like '1.2.3-5678' and return corresponding tuple (1, 2, 3, 5678)."""
    return tuple(int(n) for n in re.split('\\.|-', build_str))


def lookup_address(address: Union[bytes, str], port: Union[bytes, str, int] = None) -> list[str]:
    """Resolve address and port combination into a list of IP adresses.

    Rationale:
       - Keep syncgateway, memcached and rabbitmq services headless and simplify
         functionality to get pod IP in k8s deployment.
       - Provide a lightweight unified way of resolving adresses, both external and internal to k8s.
       - Avoid running nslookup, as it may not exist on a machine where this code is running.

    Note: For this to work in a k8s internal deployment, it should be called from code
    running inside a k8s cluster. For extrenal deployment, this will work from anywhere
    where the ingress is accessible.
    """
    entries = socket.getaddrinfo(address, port)
    return [entry[-1][0] for entry in entries]


def get_cloud_storage_bucket_stats(bucket_uri: str, aws_profile: str = "") -> tuple[int, int]:
    """Return (number of objects, total size in bytes) for a cloud storage bucket.

    Returns (-1, -1) on failure to get stats.
    """
    objects, size = -1, -1
    scheme = bucket_uri.split("://")[0].lower()
    logger.info(f"Getting stats for {bucket_uri}")

    if scheme == "s3":
        stdout = run_aws_cli_command(
            f"s3 ls {bucket_uri} --recursive --summarize | tail -2", profile=aws_profile
        )
        if stdout:
            objects, size = tuple(int(line.split()[-1]) for line in stdout.splitlines()[:2])
    elif scheme == "gs":
        stdout, _, rc = run_local_shell_command(
            f"gcloud storage du {bucket_uri} | "
            "awk 'BEGIN {{c=0;s=0}} !/\\/$/ {{c+=1;s+=$1}} END {{print c, s}}'"
        )
        if rc == 0:
            objects, size = tuple(int(n) for n in stdout.split()[:2])

    if objects > 0:
        logger.info(
            f"Stats for {bucket_uri}: {objects} objects, {size} bytes ({human_format(size, 2)}B)"
        )
    else:
        logger.error(f"Failed to get stats for {bucket_uri}")

    return objects, size


def my_public_ip() -> str:
    """Get the public IP address of the current machine."""
    resp = requests.get("https://api.ipify.org")
    resp.raise_for_status()
    return resp.content.decode()


def creds_tuple(creds: str) -> tuple[str, str]:
    """Turn a string like "<user>:<pwd>" into a tuple (<user>, <pwd>)."""
    return tuple(creds.split(":")) if ":" in creds else ("", "")


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
        file = os.path.join(self.INBOX, f"{key_name}.key")
        with open(file, "wb") as file:
            file.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))

    def _store_cert(self, cert_name: str, cert: Certificate):
        file = os.path.join(self.INBOX, f"{cert_name}.pem")
        with open(file, "wb") as file:
            file.write(cert.public_bytes(serialization.Encoding.PEM))

    def _get_general_name(self, host: str) -> GeneralName:
        # If the host address can pass as an IP v4 or v6 mark it as an IPAddress,
        # DNS name otherwise
        try:
            return IPAddress(ipaddress.ip_address(host))
        except Exception:
            return DNSName(host)
