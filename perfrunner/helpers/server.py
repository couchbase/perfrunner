from dataclasses import dataclass

from logger import logger
from perfrunner.helpers.misc import create_build_tuple
from perfrunner.helpers.rest import RestType
from perfrunner.settings import ClusterSpec


@dataclass(frozen=True)
class ServerInfo:
    _INVALID = "0.0.0.0"  # Sentinel value for invalid server
    build: str = "0.0.0"
    build_tuple: tuple[int] = (0, 0, 0)
    is_columnar: bool = False
    is_community: bool = False
    master_node: str = _INVALID
    raw_version: str = "0.0.0"

    def is_valid(self) -> bool:
        return self.master_node != self._INVALID

    def __str__(self) -> str:
        return f"version={self.raw_version}, is_columnar={self.is_columnar}"


class ServerInfoManager:
    """Singleton manager for server information to avoid reprocessing across classes.

    This class provides a centralised way to manage server information such as version, master node
    and others across different parts of perfrunner. It stores the information in a dictionary
    to avoid redundant REST API calls and provides thread-safe access to server information.

    The server information for all clusters in the spec is intialised once on creation of the
    first instance of the class. To refresh the server information, the `refresh_server_info` method
    can be used.

    Usage:
        # Basic usage
        ```
        manager = ServerInfoManager(cluster_spec, rest_helper)
        server_info = manager.get_server_info()
        ```
        # Using as context manager
        ```
        with ServerInfoManager(cluster_spec, rest_helper) as manager:
            server_info = manager.get_server_info()
        ```
        # Get server information for a specific cluster (ordered by their appearance in the spec)

        ```
        server_info = manager.get_server_info(index=0)
        ```
        # Get server information for a specific master node
        ```
        server_info = manager.get_server_info_by_master_node(master_node="10.0.0.1")
        ```

    Args:
        cluster_spec: Cluster specification containing master nodes
        rest_helper: REST helper instance for API calls

    Benefits:
        - Eliminates redundant REST API calls
        - Provides consistent server information across classes
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialise(*args, **kwargs)
        return cls._instance

    def _initialise(self, cluster_spec: ClusterSpec, rest_helper: RestType):
        self._server_info = {}
        if cluster_spec is None or rest_helper is None:
            logger.warning(
                "Cluster spec or rest helper not provided, skipping server info initialisation"
            )
            return
        for master_node in cluster_spec.masters:
            self._server_info[master_node] = self._initialise_server_info(master_node, rest_helper)
            logger.info(f"Couchbase Server on {master_node}: {self._server_info[master_node]}")

        if not self._server_info:
            logger.warning("No clusters found in the spec")
            self._server_info[ServerInfo._INVALID] = ServerInfo()

    def _initialise_server_info(self, master_node: str, rest_helper: RestType) -> ServerInfo:
        raw_version = rest_helper.get_version_raw(master_node)
        version = (
            raw_version.replace("-rel-enterprise", "")
            .replace("-enterprise", "")
            .replace("-community", "")
            .replace("-columnar", "")
        )
        return ServerInfo(
            build=version,
            raw_version=raw_version,
            build_tuple=create_build_tuple(version),
            is_columnar="columnar" in raw_version,
            master_node=master_node,
            is_community="community" in raw_version,
        )

    def get_server_info(self, index: int = 0) -> ServerInfo:
        """Get server information for a specific cluster.

        Args:
            index: Index of the cluster to get information for. Clusters are ordered by their
            appearance in the cluster specification.

        Returns:
            ServerInfo: Server information for the cluster.
        """
        # We will have at least one server in a test (or a cluster with no servers)
        return list(self._server_info.values())[index]

    def get_server_info_by_master_node(self, master_node: str) -> ServerInfo:
        """Get server information for a specific master node.

        The master node in perfrunner terms is the first node listed on the cluster.
        """
        return self._server_info.get(master_node, ServerInfo())

    def refresh_server_info(self, cluster_spec: ClusterSpec, rest_helper: RestType):
        """Refresh server information for all clusters."""
        self._server_info.clear()
        self._initialise(cluster_spec, rest_helper)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
