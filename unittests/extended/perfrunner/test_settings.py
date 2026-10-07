"""Which host serves each Sync Gateway API, as derived from the cluster spec."""

from tempfile import NamedTemporaryFile

import pytest

from perfrunner.settings import ClusterSpec


def parse_spec(spec: str) -> ClusterSpec:
    with NamedTemporaryFile("w", suffix=".spec") as spec_file:
        spec_file.write(spec)
        spec_file.flush()
        cluster_spec = ClusterSpec()
        cluster_spec.parse(spec_file.name, override=None)
    return cluster_spec


def sgw_spec(provider: str, backend: str, host: str) -> str:
    return (
        "[infrastructure]\n"
        f"provider = {provider}\n"
        f"backend = {backend}\n"
        "\n"
        "[syncgateways]\n"
        "syncgateways1 =\n"
        f"    {host}\n"
        f"    {host}\n"
        "\n"
        "[client_private_ips]\n"
        "workers1 = 10.1.0.3\n"
    )


@pytest.mark.parametrize(
    "provider, backend, host, api, expected",
    [
        # GCP App Services serves each API from its own host, all on port 443
        ("capella", "gcp", "abc123.apps.example.com", "admin", "abc123-admin.apps.example.com"),
        ("capella", "gcp", "abc123.apps.example.com", "metric", "abc123-metric.apps.example.com"),
        (
            "capella", "gcp", "abc123-admin.apps.example.com", "admin",
            "abc123-admin.apps.example.com",
        ),
        # Elsewhere the APIs share the host and differ by port
        ("capella", "aws", "abc123.apps.example.com", "admin", "abc123.apps.example.com"),
        ("capella", "azure", "abc123.apps.example.com", "admin", "abc123.apps.example.com"),
        ("aws", "aws", "sgw1.example.com", "admin", "sgw1.example.com"),
        ("", "", "172.23.96.1", "admin", "172.23.96.1"),
    ],
)
def test_sgw_api_host(provider: str, backend: str, host: str, api: str, expected: str):
    cluster_spec = parse_spec(sgw_spec(provider, backend, host))

    assert cluster_spec.sgw_api_host(host, api) == expected
