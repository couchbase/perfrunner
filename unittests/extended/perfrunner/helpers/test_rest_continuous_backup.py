"""The continuous backup request, which the existing tools tests also send."""

import os
from unittest import TestCase

os.environ.setdefault("WORKER_TYPE", "local")

from perfrunner.helpers.rest import DefaultRestHelper  # noqa: E402


class ConfigureContinuousBackupTest(TestCase):
    def configure(self, **kwargs):
        rest = object.__new__(DefaultRestHelper)
        rest._get_api_url = lambda **kw: kw["path"]
        posted = []
        rest.post = lambda url, data=None, **kw: posted.append(data)
        rest.configure_continuous_backup(
            "h", "bucket-1", "true", backup_location="/loc", backup_interval=2, **kwargs
        )
        return posted[0]

    def test_optional_fields_are_left_out_for_the_tools_tests(self):
        """Older servers may reject fields they do not know."""
        data = self.configure()

        self.assertEqual(
            set(data),
            {
                "continuousBackupEnabled",
                "continuousBackupLocation",
                "continuousBackupInterval",
                "name",
            },
        )

    def test_retention_and_key_management_are_sent_when_given(self):
        data = self.configure(retention_minutes=60, km_cred_id="c1", km_key_url="awskms://k")

        self.assertEqual(data["continuousBackupRetentionPeriod"], 60)
        self.assertEqual(data["continuousBackupKmCredId"], "c1")
        self.assertEqual(data["continuousBackupKmKeyUrl"], "awskms://k")
