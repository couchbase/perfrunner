from cbagent.collectors.collector import Collector

from cbagent.collectors.active_tasks import ActiveTasks
from cbagent.collectors.analytics import AnalyticsStats
from cbagent.collectors.eventing_stats import (
    EventingConsumerStats,
    EventingPerNodeStats,
    EventingStats,
)
from cbagent.collectors.fts_stats import (
    FTSCollector,
    ElasticStats,
)
from cbagent.collectors.jts_stats import JTSCollector
from cbagent.collectors.kvstore_stats import KVStoreStats
from cbagent.collectors.latency import Latency, KVLatency, QueryLatency
from cbagent.collectors.observe import (
    DurabilityLatency,
    ObserveIndexLatency,
    ObserveSecondaryIndexLatency,
)
from cbagent.collectors.n1ql_stats import N1QLStats
from cbagent.collectors.ns_server import NSServer, NSServerOverview, NSServerSystem, XdcrStats
from cbagent.collectors.secondary_debugstats import (
    SecondaryDebugStats,
    SecondaryDebugStatsBucket,
    SecondaryDebugStatsIndex,
)
from cbagent.collectors.secondary_latency import SecondaryLatencyStats
from cbagent.collectors.secondary_stats import SecondaryStats
from cbagent.collectors.secondary_storage_stats import SecondaryStorageStats
from cbagent.collectors.secondary_storage_stats_mm import SecondaryStorageStatsMM
from cbagent.collectors.system import (
    Disk,
    IO,
    Memory,
    Net,
    PageCache,
    PS,
    Sysdig,
    TypePerf,
)
from cbagent.collectors.xdcr_lag import XdcrLag
