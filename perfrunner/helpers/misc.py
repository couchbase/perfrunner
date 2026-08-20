import base64
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
from typing import Any, NamedTuple, Optional, Union
from uuid import uuid4

import requests
import validators
import yaml
from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, generate_private_key
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509 import (
    AuthorityKeyIdentifier,
    BasicConstraints,
    Certificate,
    CertificateBuilder,
    CertificateRevocationList,
    CertificateRevocationListBuilder,
    CertificateSigningRequest,
    CertificateSigningRequestBuilder,
    CRLNumber,
    DNSName,
    ExtendedKeyUsage,
    ExtendedKeyUsageOID,
    ExtensionNotFound,
    GeneralName,
    IPAddress,
    KeyUsage,
    Name,
    NameAttribute,
    RevokedCertificateBuilder,
    SubjectAlternativeName,
    SubjectKeyIdentifier,
    load_pem_x509_certificate,
    load_pem_x509_crl,
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
        return "{}{}".format(
            self.min_port, f"-{self.max_port}" if self.max_port != self.min_port else ""
        )

    def __str__(self) -> str:
        return f"(ports={self.port_range_str()}, protocol={self.protocol})"


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


def try_json_decode(value: str) -> dict:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        logger.warning(f"Failed to decode JSON from value: {value}")
        return {}


def read_yaml(filename: str) -> dict:
    with open(filename) as fh:
        return yaml.safe_load(fh)


def sort_bucket_key(bucket: str, bucket_size: int = 1000, bucket_count: int = 100) -> float:
    if bucket.startswith('>'):
        return float("inf")
    if '-' in bucket:
        start = bucket.split('-')[0]
        if bucket.endswith('ms'):
            start_us = int(start) * 1000  # convert ms -> us for unified comparison
        elif bucket.endswith('us'):
            start_us = int(start)
        else:
            return 0
        if start_us > bucket_count * bucket_size:
            return float("inf")
        return start_us
    return 0


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


DURATION_UNITS_TO_SECS = {
    "h": 3600.0,
    "m": 60.0,
    "s": 1.0,
    "ms": 1e-3,
    "us": 1e-6,
    "μs": 1e-6,
    "µs": 1e-6,
    "ns": 1e-9,
}


def parse_duration_to_secs(duration: str) -> float:
    """Convert a duration string to seconds, e.g. '250ms' -> 0.25, '1m20.5s' -> 80.5.

    A bare number is read as seconds. Compound durations are summed. NaN is returned unless the
    whole string is consumed, so an unrecognised unit is never silently read as seconds.
    """
    if not (duration := duration.strip()):
        logger.error("Cannot parse duration from an empty string")
        return float("nan")

    total_secs, pos = 0.0, 0
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*([a-zA-Zμµ]*)\s*", duration):
        if m.start() != pos:
            break  # Non-numeric junk between components: bail out and report below
        unit = (m.group(2) or "s").lower()
        if (multiplier := DURATION_UNITS_TO_SECS.get(unit)) is None:
            logger.error(f"Unknown duration unit '{unit}' in duration: {duration}")
            return float("nan")
        total_secs += float(m.group(1)) * multiplier
        pos = m.end()

    if pos != len(duration):
        logger.error(f"Failed to parse duration: {duration}")
        return float("nan")

    return total_secs


def copy_template(source, dest):
    shutil.copyfile(source, dest)


def generate_bedrock_api_key(access_key: str, secret_key: str, region: str) -> str:
    """Generate a short-term AWS Bedrock API key (bearer token) from IAM credentials.

    Replicates the official `aws-bedrock-token-generator` package, which we cannot depend on
    directly because it requires a newer botocore than the current awscli pinned allows.
    Tokens are valid for 12 hours, so they must be generated at test run time rather than stored.
    """
    request = AWSRequest(
        method="POST",
        url="https://bedrock.amazonaws.com/",
        headers={"host": "bedrock.amazonaws.com"},
        params={"Action": "CallWithBearerToken"},
    )
    auth = SigV4QueryAuth(Credentials(access_key, secret_key), "bedrock", region, expires=43200)
    auth.add_auth(request)
    presigned_url = f"{request.url.removeprefix('https://')}&Version=1"
    return f"bedrock-api-key-{base64.b64encode(presigned_url.encode()).decode()}"


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


def remove_nulls(d: Union[dict, Any]) -> Union[dict, Any]:
    """Remove None-valued keys from a dict."""
    if not isinstance(d, dict):
        return d
    return {k: new_v for k, v in d.items() if not is_null(new_v := remove_nulls(v))}


def run_local_shell_command(
    command: Union[str, list[str]],
    *,
    success_msg: str = "",
    err_msg: str = "",
    quiet: bool = False,
    raise_error: bool = False,
    **popen_kwargs: dict,
) -> tuple[Optional[str], Optional[str], int]:
    """Run a shell command locally using `subprocess` and print stdout + stderr on failure.

    Args
    ----
    command: shell command to run (as a string or list of args)
    success_msg: message to print on success
    err_msg: message to print on failure (in addition to command stdout and stderr)
    quiet: if True, don't print stdout and stderr on failure
    raise_error: if True, raise exception if the command fails
    **popen_kwargs: additional kwargs to pass to `subprocess.run`

    Returns
    -------
    command stdout, stderr, return code

    """
    popen_kwargs |= {
        "shell": isinstance(command, str),
        "env": os.environ | popen_kwargs.get("env", {}),
    }
    if capture_output := (not popen_kwargs.get("stdout") and not popen_kwargs.get("stderr")):
        popen_kwargs["capture_output"] = True

    process = subprocess.run(command, **popen_kwargs)

    stdout, stderr = None, None
    if capture_output:
        stdout, stderr = process.stdout.decode(), process.stderr.decode()

    if (returncode := process.returncode) == 0:
        if success_msg:
            logger.info(success_msg)
    elif not quiet:
        if err_msg:
            logger.error(err_msg)
        logger.error(f"Command failed with return code {returncode}: {process.args}")
        logger.error(f"Captured stdout: {stdout}")
        logger.error(f"Captured stderr: {stderr}")
        if raise_error:
            raise Exception(f"Command failed with return code {returncode}: {process.args}")

    return stdout, stderr, returncode


def get_max_arg_strlen() -> int:
    stdout, _, _ = run_local_shell_command("getconf PAGE_SIZE")
    return int(stdout.strip()) * 32


def set_azure_subscription(sub_name: str, alias: str) -> int:
    _, _, err = run_local_shell_command(
        command=f'az account set --subscription "{sub_name}"',
        success_msg=f'Set active Azure subscription to "{sub_name}" ({alias})',
        err_msg=f'Failed to set active Azure subscription to "{sub_name}" ({alias})',
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


def get_azure_storage_account_key(storage_acc: str) -> str:
    stdout, _, _ = run_local_shell_command(
        f"az storage account keys list --account-name {storage_acc} "
        "--query '[0].value' --output tsv"
    )
    return stdout.strip()


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
        return f'"{version}"'
    elif 'refs/changes' in version:  # gerrit change
        return f"git+https://review.couchbase.org/couchbase-python-client@{version}"
    elif '.' not in version:  # git commit
        return f"git+https://github.com/couchbase/couchbase-python-client.git@{version}"
    else:
        return f"couchbase=={version}"


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


def get_cloud_storage_bucket_stats(
    bucket_uri: str, aws_profile: str = "", az_storage_acc: str = ""
) -> tuple[int, int]:
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
    else:
        if scheme == "gs":
            command = (
                f"gcloud storage du {bucket_uri} "
                "| awk 'BEGIN {c=0;s=0} !/\\/$/ {c++;s+=$1} END {print c, s}'"
            )
        elif scheme in ("azblob", "az"):
            container = bucket_uri.split("/")[-1]
            command = (
                f'az storage blob list --num-results "*" --account-name {az_storage_acc} '
                f"--container-name {container} --query '[].properties.contentLength' --output tsv "
                "| awk 'BEGIN {c=0;s=0} {c++;s+=$1} END {print c, s}'"
            )

        stdout, _, rc = run_local_shell_command(command)
        if rc == 0:
            objects, size = tuple(int(n) for n in stdout.split()[:2])

    if objects >= 0:
        logger.info(
            f"Stats for {bucket_uri}: {objects} objects, {size} bytes ({human_format(size, 2)}B)"
        )
    else:
        logger.error(f"Failed to get stats for {bucket_uri}")

    return objects, size


def my_public_ip() -> str:
    """Get the public IP address of the current machine."""
    urls = [
        "checkip.amazonaws.com",
        "api.ipify.org",
        "ifconfig.me/ip",
    ]
    retries = 3
    delay = 5
    for i in range(retries):
        for url in urls:
            try:
                resp = requests.get(f"https://{url}", timeout=10)
                resp.raise_for_status()
                ip = resp.content.decode().strip()
                assert ipaddress.ip_address(ip)
                return ip
            except Exception as e:
                logger.warning(f"Failed to get public IP address from {url}: {e}")
                continue

        if i < retries - 1:
            time.sleep(delay**retries)

    logger.interrupt(f"Failed to get public IP address from {urls} after {retries} retries")


def creds_tuple(creds: str) -> tuple[str, str]:
    """Turn a string like "<user>:<pwd>" into a tuple (<user>, <pwd>)."""
    return tuple(creds.split(":")) if ":" in creds else ("", "")


class X509CertPathPair(NamedTuple):
    cert_path: str
    key_path: str


class SSLCertificate:

    """Generate X.509 certificates for either cluster nodes or clients.

    Calling `generate_server_cert()` will create a node certificate with SAN including all node IPs.

    Calling `generate_client_cert()` will create a client cert with CN set to the desired username
    for authenticating with the cluster, stored both as a PEM pair and as a PKCS#12 bundle: the
    Python SDK and `requests` take the PEM pair, `keytool` can only import the bundle.

    Calling `generate_crl()` will create a (by default empty) CRL for the CA, for tests that
    exercise revocation checking.

    A self-signed root certificate is generated to sign node/client certificates. If a CA is
    already present in the output directory it is reused, so node and client certificates
    generated by separate calls share the same trust chain.

    Everything is written to `INBOX` unless the caller passes `output_dir`. Only the standalone
    `x509_cert` CLI does: the perfrunner flow reads the artefacts back through the `*_PATH`
    constants, which name the inbox and nothing else.
    """

    INBOX_DIRNAME = "inbox"
    INBOX = f"certificates/{INBOX_DIRNAME}"

    # The CN has to name an existing Couchbase RBAC user: `enable_certificate_auth` maps the whole
    # CN to the username. `add_rbac_users` creates one user per bucket (with the admin role), and
    # runs before certificate setup, so the first bucket is a safe default. It is also the identity
    # the static certificates this replaced used to carry.
    DEFAULT_CLIENT_CN = "bucket-1"
    DEFAULT_STOREPASS = "storepass"

    # The PKCS#12 friendlyName, which is the alias `keytool` addresses the entry by.
    CLIENT_BUNDLE_ALIAS = "client"

    CERT_VALIDITY = datetime.timedelta(days=60)
    MIN_CA_VALIDITY = datetime.timedelta(days=7)

    # File names are useful on their own: certificates are staged under bare names elsewhere
    # (on cluster nodes and on workload workers), where the inbox path does not apply.
    CA_CERT_FILENAME = "ca.pem"
    CA_KEY_FILENAME = "ca.key"
    CRL_FILENAME = "ca.crl"

    SERVER_CERT_FILENAME = "chain.pem"
    SERVER_KEY_FILENAME = "pkey.key"

    CLIENT_CERT_FILENAME = "client.pem"
    CLIENT_KEY_FILENAME = "client.key"
    CLIENT_BUNDLE_FILENAME = "client.p12"

    # Where readers outside this class find each artefact. Writing goes through `path()`
    # instead, so `output_dir` can point elsewhere; these stay pinned to the inbox.
    CA_CERT_PATH = f"{INBOX}/{CA_CERT_FILENAME}"
    CA_KEY_PATH = f"{INBOX}/{CA_KEY_FILENAME}"
    CRL_PATH = f"{INBOX}/{CRL_FILENAME}"

    SERVER_CERT_PATH = f"{INBOX}/{SERVER_CERT_FILENAME}"
    SERVER_KEY_PATH = f"{INBOX}/{SERVER_KEY_FILENAME}"

    CLIENT_CERT_PATH = f"{INBOX}/{CLIENT_CERT_FILENAME}"
    CLIENT_KEY_PATH = f"{INBOX}/{CLIENT_KEY_FILENAME}"
    CLIENT_BUNDLE_PATH = f"{INBOX}/{CLIENT_BUNDLE_FILENAME}"

    def __init__(self, hosts: Optional[list[str]] = None, output_dir: str = INBOX):
        self.hosts = hosts or ["127.0.0.1"]
        self.output_dir = output_dir
        self.not_valid_before = datetime.datetime.now(datetime.timezone.utc)
        self.not_valid_after = self.not_valid_before + self.CERT_VALIDITY

    def path(self, filename: str) -> str:
        """Where this instance writes `filename`, and where it looks for a CA to reuse."""
        return os.path.join(self.output_dir, filename)

    def generate_server_cert(self):
        """
        Generate signed node certificate and store it and its key to the output directory.

        If CA certificate and private key aren't already there, generate them too.
        """
        logger.info('Generating certificates')
        # Generate CA first
        ca_key, ca = self.maybe_generate_ca()

        # Certificate signing request. Both serverAuth and clientAuth: under node-to-node
        # encryption a node is also a TLS client of other nodes (the master connecting to a node
        # being added during rebalance, for instance), which serverAuth alone does not permit.
        request_key, req = self.generate_signing_request(
            ca,
            "Couchbase Server",
            [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
        )

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
        self._store_key(self.path(self.SERVER_KEY_FILENAME), request_key)
        self._store_cert(self.path(self.SERVER_CERT_FILENAME), cert)

    def generate_client_cert(
        self, username: str = DEFAULT_CLIENT_CN, storepass: str = DEFAULT_STOREPASS
    ) -> X509CertPathPair:
        """
        Generate signed client certificate and store it and its key to the output directory.

        The certificate and key are stored as a PEM pair and as a PKCS#12 bundle encrypted with
        `storepass`. The PEM pair is what the Python SDK and `requests` take; `keytool` can only
        import the bundle, so both are written from the same key pair.

        If CA certificate and private key aren't already there, generate them too.
        """
        ca_key, ca = self.maybe_generate_ca()

        # Certificate signing request
        client_key, client_csr = self.generate_signing_request(
            ca, username, [ExtendedKeyUsageOID.CLIENT_AUTH]
        )

        # Sign the client certificate
        client_cert = CertificateBuilder(
            issuer_name=ca.subject,
            subject_name=client_csr.subject,
            public_key=client_csr.public_key(),
            serial_number=random_serial_number(),
            not_valid_before=self.not_valid_before,
            not_valid_after=self.not_valid_after,
            extensions=client_csr.extensions,
        ).sign(ca_key, hashes.SHA256())

        # Store individual files
        key_filepath = self._store_key(self.path(self.CLIENT_KEY_FILENAME), client_key)
        cert_filepath = self._store_cert(self.path(self.CLIENT_CERT_FILENAME), client_cert)
        self._store_pkcs12(client_key, client_cert, ca, storepass)

        return X509CertPathPair(cert_filepath, key_filepath)

    def generate_crl(self, revoked_serial_numbers: Optional[list[int]] = None) -> str:
        """
        Generate a CRL for the CA and store it, revoking the given serial numbers.

        The CRL is empty unless serial numbers are passed, so that tests enforcing revocation
        checking have something to upload from the start. Returns the path it was stored at.

        If CA certificate and private key aren't already there, generate them too.
        """
        ca_key, ca = self.maybe_generate_ca()

        builder = (
            CertificateRevocationListBuilder()
            .issuer_name(ca.subject)
            .last_update(self.not_valid_before)
            # Deliberately the certificate lifetime rather than something shorter: past
            # nextUpdate a validator may treat the CRL as stale and, under a policy that
            # requires CRLs, fail closed part-way through a long-running test.
            .next_update(self.not_valid_after)
            .add_extension(CRLNumber(self._next_crl_number()), critical=False)
        )

        for serial_number in revoked_serial_numbers or []:
            builder = builder.add_revoked_certificate(
                RevokedCertificateBuilder()
                .serial_number(serial_number)
                .revocation_date(self.not_valid_before)
                .build()
            )

        return self._store_crl(self.path(self.CRL_FILENAME), builder.sign(ca_key, hashes.SHA256()))

    def generate_ca(self) -> tuple[RSAPrivateKey, Certificate]:
        """Generate a certificate that can be used as a Certificate Authority (CA)."""
        ca_key_pair = self._make_key_pair()
        subject = self._get_subject('Couchbase Root CA')
        ca = (
            CertificateBuilder(
                issuer_name=subject,
                subject_name=subject,
                public_key=ca_key_pair.public_key(),
                serial_number=random_serial_number(),
                not_valid_before=self.not_valid_before,
                not_valid_after=self.not_valid_after,
            )
            .add_extension(BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                SubjectKeyIdentifier.from_public_key(ca_key_pair.public_key()), critical=False
            )
            .add_extension(
                # crl_sign: this CA signs the CRL in `generate_crl()`, so its KeyUsage has to assert
                # cRLSign (RFC 5280 4.2.1.3) for validators to accept it as the CRL issuer.
                KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(ca_key_pair, hashes.SHA256())
        )

        # Store CA
        self._store_cert(self.path(self.CA_CERT_FILENAME), ca)
        self._store_key(self.path(self.CA_KEY_FILENAME), ca_key_pair)
        return ca_key_pair, ca

    def generate_signing_request(
        self, ca: Certificate, common_name: str, eku_oids: list[ExtendedKeyUsageOID]
    ) -> tuple[RSAPrivateKey, CertificateSigningRequest]:
        """Generate a CSR and a private key that can be used to generate a certificate."""
        pkey = self._make_key_pair()
        server_auth = ExtendedKeyUsageOID.SERVER_AUTH in eku_oids

        req = (
            CertificateSigningRequestBuilder()
            .subject_name(self._get_subject(common_name))
            .add_extension(BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(SubjectKeyIdentifier.from_public_key(pkey.public_key()), critical=False)
            .add_extension(
                AuthorityKeyIdentifier.from_issuer_public_key(ca.public_key()), critical=False
            )
            .add_extension(
                KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=server_auth,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(ExtendedKeyUsage(eku_oids), critical=False)
        )

        if server_auth:
            san_list = [self._get_general_name(host) for host in self.hosts]
            req = req.add_extension(SubjectAlternativeName(san_list), critical=False)

        req = req.sign(pkey, hashes.SHA256())

        return pkey, req

    def maybe_generate_ca(self) -> tuple[RSAPrivateKey, Certificate]:
        if self._check_if_ca_exists():
            ca_key, ca = self.load_ca()
            if self._ca_is_usable(ca):
                self.not_valid_after = min(self.not_valid_after, ca.not_valid_after_utc)
                return ca_key, ca
            # Jenkins checks the repo out from scratch for every run, so the inbox is normally
            # empty to begin with, but a local or repeated run against an existing checkout can
            # find a CA too old to sign with. Reusing it silently would produce certificates that
            # fail every handshake for no visible reason.
            logger.info(
                f"Replacing the CA in {self.output_dir}: it is valid "
                f"{ca.not_valid_before_utc:%Y-%m-%d} to {ca.not_valid_after_utc:%Y-%m-%d}, "
                f"which leaves less than {self.MIN_CA_VALIDITY} of validity"
            )
        return self.generate_ca()

    def load_ca(self) -> tuple[RSAPrivateKey, Certificate]:
        with open(self.path(self.CA_KEY_FILENAME), "rb") as f:
            root_ca_key = serialization.load_pem_private_key(f.read(), password=None)
        with open(self.path(self.CA_CERT_FILENAME), "rb") as f:
            root_ca_cert = load_pem_x509_certificate(f.read())
        return root_ca_key, root_ca_cert

    def _check_if_ca_exists(self) -> bool:
        return all(
            os.path.exists(self.path(name))
            for name in (self.CA_CERT_FILENAME, self.CA_KEY_FILENAME)
        )

    def _ca_is_usable(self, ca: Certificate) -> bool:
        """Whether `ca` is currently valid and has `MIN_CA_VALIDITY` left to sign with.

        Deliberately not "outlives the certificates it signs": a CA is reused across calls, so
        it is always slightly older than what it signs, and requiring full coverage would mean
        never reusing one.
        """
        return (
            ca.not_valid_before_utc <= self.not_valid_before
            and ca.not_valid_after_utc >= self.not_valid_before + self.MIN_CA_VALIDITY
        )

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

    def _store_file(self, filepath: str, file_bytes: bytes, mode: int = 0o644) -> str:
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with os.fdopen(
            os.open(filepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode), "wb"
        ) as file:
            os.fchmod(file.fileno(), mode)
            file.write(file_bytes)
        return filepath

    def _store_key(self, filepath: str, key: RSAPrivateKey) -> str:
        return self._store_file(
            filepath,
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ),
            0o600,
        )

    def _store_cert(self, filepath: str, cert: Certificate) -> str:
        return self._store_file(filepath, cert.public_bytes(serialization.Encoding.PEM))

    def _store_pkcs12(
        self, key: RSAPrivateKey, cert: Certificate, ca: Certificate, storepass: str
    ) -> str:
        filepath = self.path(self.CLIENT_BUNDLE_FILENAME)
        # PBES1/3DES rather than the modern AES-256/PBES2 default: `keytool` has to read this
        # back on the workers, and older JREs cannot decrypt PBES2 bundles.
        encryption = (
            serialization.PrivateFormat.PKCS12.encryption_builder()
            .key_cert_algorithm(pkcs12.PBES.PBESv1SHA1And3KeyTripleDESCBC)
            .hmac_hash(hashes.SHA1())
            .build(storepass.encode())
        )
        return self._store_file(
            filepath,
            pkcs12.serialize_key_and_certificates(
                name=self.CLIENT_BUNDLE_ALIAS.encode(),
                key=key,
                cert=cert,
                cas=[ca],
                encryption_algorithm=encryption,
            ),
            0o600,
        )

    def _store_crl(self, filepath: str, crl: CertificateRevocationList) -> str:
        return self._store_file(filepath, crl.public_bytes(serialization.Encoding.PEM))

    def _next_crl_number(self) -> int:
        """Read previous CRL if one exists and return next valid CRL number.

        CRL numbers have to increase (RFC 5280 5.2.3) or a relying party holding an earlier CRL
        can't tell whether a new one supersedes it.
        """
        path = self.path(self.CRL_FILENAME)
        if not os.path.exists(path):
            return 1

        with open(path, "rb") as f:
            previous = load_pem_x509_crl(f.read())

        try:
            return previous.extensions.get_extension_for_class(CRLNumber).value.crl_number + 1
        except ExtensionNotFound:
            return 1

    def _get_general_name(self, host: str) -> GeneralName:
        # If the host address can pass as an IP v4 or v6 mark it as an IPAddress,
        # DNS name otherwise
        try:
            return IPAddress(ipaddress.ip_address(host))
        except Exception:
            return DNSName(host)
