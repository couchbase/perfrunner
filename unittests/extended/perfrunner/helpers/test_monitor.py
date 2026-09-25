"""Item counting in Monitor.get_num_items() and Monitor.monitor_num_items()."""

from unittest import TestCase
from unittest.mock import patch

from perfrunner.helpers.monitor import Monitor


class FakeRest:
    REPLICA_NUMBER = 1

    def __init__(self, active_items: dict[str, int], collection_items: dict[tuple, int]):
        # Every item, ignored ones included, has REPLICA_NUMBER replicas
        self.active_items = active_items
        self.collection_items = collection_items

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        active = self.active_items[bucket]
        samples = {"curr_items": [active], "curr_items_tot": [active * (1 + self.REPLICA_NUMBER)]}
        return {"op": {"samples": samples}}

    def get_scope_and_collection_items(self, host: str, bucket: str, scope: str, coll=None):
        items = self.collection_items.get((bucket, scope))
        return {"data": [{"values": [[0, str(items)]]}] if items else []}

    def get_bucket_info(self, host: str, bucket: str) -> dict:
        return {"replicaNumber": self.REPLICA_NUMBER}


class GetNumItemsTest(TestCase):
    """Items in `_system` and the query AWR scope are not counted, and only in their own bucket."""

    def setUp(self):
        self.monitor = Monitor.__new__(Monitor)
        self.monitor.awr_bucket = "bucket-1"
        self.monitor.awr_scope = "scope-awr"
        self.monitor.rest = FakeRest(
            active_items={"bucket-1": 1001, "bucket-2": 1003},
            collection_items={("bucket-1", "scope-awr"): 1, ("bucket-2", "_system"): 3},
        )

    def test_awr_items_are_ignored(self):
        self.assertEqual(self.monitor.get_num_items("host", "bucket-1"), 2000)
        self.assertEqual(self.monitor.get_num_items("host", "bucket-1", False), 1000)

    def test_system_items_are_ignored(self):
        self.assertEqual(self.monitor.get_num_items("host", "bucket-2"), 2000)
        self.assertEqual(self.monitor.get_num_items("host", "bucket-2", False), 1000)

    def test_awr_items_are_not_ignored_in_other_buckets(self):
        self.monitor.rest.active_items["bucket-2"] = 1000
        self.monitor.rest.collection_items.pop(("bucket-2", "_system"))
        self.assertEqual(self.monitor.get_num_items("host", "bucket-2"), 2000)


@patch("perfrunner.helpers.monitor.time.sleep")
class MonitorNumItemsTest(TestCase):
    """Polling stops once the count is within tolerance, and fails after max_retry polls."""

    def setUp(self):
        self.monitor = Monitor.__new__(Monitor)
        self.counts = []
        # The count stays at its last value once the list runs out
        self.monitor.get_num_items = lambda host, bucket: (
            self.counts.pop(0) if len(self.counts) > 1 else self.counts[0]
        )

    def test_waits_for_exact_count_without_tolerance(self, sleep):
        self.counts = [990, 999, 1000]
        self.monitor.monitor_num_items("host", "bucket-1", 1000)
        self.assertEqual(sleep.call_count, 2)

    def test_stops_at_once_within_tolerance(self, sleep):
        self.counts = [995]
        self.monitor.monitor_num_items("host", "bucket-1", 1000, tolerance_percent=1)
        sleep.assert_not_called()

    def test_fails_outside_tolerance(self, sleep):
        self.counts = [980, 985, 989]
        with self.assertRaisesRegex(Exception, "Mismatch in the number of items: 989"):
            self.monitor.monitor_num_items("host", "bucket-1", 1000, 1, max_retry=3)
