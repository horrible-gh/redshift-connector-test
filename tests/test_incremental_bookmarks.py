import sys
import types
import unittest

# redshift_client imports these runtime packages, but SQL construction can be
# unit-tested without installing the Connector SDK or opening Redshift.
sdk = types.ModuleType("fivetran_connector_sdk")

class DummyLogging:
    @staticmethod
    def info(*args, **kwargs):
        pass

    @staticmethod
    def warning(*args, **kwargs):
        pass

    @staticmethod
    def error(*args, **kwargs):
        pass

class DummyOperations:
    @staticmethod
    def upsert(*args, **kwargs):
        pass

    @staticmethod
    def checkpoint(*args, **kwargs):
        pass

sdk.Logging = DummyLogging
sdk.Operations = DummyOperations
sys.modules.setdefault("fivetran_connector_sdk", sdk)

redshift_connector = types.ModuleType("redshift_connector")
redshift_connector.connect = lambda **kwargs: None
sys.modules.setdefault("redshift_connector", redshift_connector)

from redshift_client import build_select


class IncrementalBookmarkTests(unittest.TestCase):
    def test_pk_backed_boundary_can_be_inclusive(self):
        sql, params = build_select(
            redshift_schema="public",
            table="orders",
            columns=["id", "update_time"],
            replication_key="update_time",
            bookmark="20261111123456",
            inclusive_bookmark=True,
        )

        self.assertIn('"update_time" >= %s', sql)
        self.assertEqual(["20261111123456"], params)

    def test_non_pk_or_internal_chunk_boundary_remains_strict(self):
        sql, params = build_select(
            redshift_schema="public",
            table="orders",
            columns=["id", "update_time"],
            replication_key="update_time",
            bookmark="20261111123456",
            inclusive_bookmark=False,
        )

        self.assertIn('"update_time" > %s', sql)
        self.assertNotIn('"update_time" >= %s', sql)
        self.assertEqual(["20261111123456"], params)

    def test_inclusive_lower_bound_and_chunk_upper_bound_can_coexist(self):
        sql, params = build_select(
            redshift_schema="public",
            table="orders",
            columns=["id", "update_time"],
            replication_key="update_time",
            bookmark="20261111123456",
            upper_bound="20261112100000",
            inclusive_bookmark=True,
        )

        self.assertIn('"update_time" >= %s', sql)
        self.assertIn('"update_time" <= %s', sql)
        self.assertEqual(["20261111123456", "20261112100000"], params)


if __name__ == "__main__":
    unittest.main()
