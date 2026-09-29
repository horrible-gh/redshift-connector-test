import sys
import types
import unittest
from unittest.mock import patch

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

from redshift_client import (
    _checkpoint,
    _determine_strategy_and_replication_key,
    build_select,
    sync_table_chunked_cursors,
)


class DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        pass


class DummyConnection:
    def cursor(self):
        return DummyCursor()


class DummyPlan:
    stream = "public.orders"
    schema = "public"
    table = "orders"
    replication_key = "update_time"
    primary_keys = ["id"]
    selected_columns = ["id", "update_time"]
    filter_condition = None


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


    def test_chunk_target_uses_configured_batch_size(self):
        captured = {}

        def fake_find_upper_bound(
            connection,
            plan,
            replication_key,
            bookmark,
            chunk_size,
            filter_condition=None,
            inclusive_bookmark=False,
        ):
            captured["chunk_size"] = chunk_size
            return None

        with patch(
            "redshift_client._find_chunk_upper_bound",
            side_effect=fake_find_upper_bound,
        ), patch(
            "redshift_client._declare_cursor"
        ), patch(
            "redshift_client.upsert_record",
            return_value=({}, None, 0),
        ), patch(
            "redshift_client._checkpoint"
        ):
            sync_table_chunked_cursors(
                connection=DummyConnection(),
                plan=DummyPlan(),
                state={},
                bookmark=None,
                batch_size=25000,
            )

        self.assertEqual(25000, captured["chunk_size"])


    def test_numeric_bookmark_preserves_integer_type(self):
        state = {}
        _checkpoint(
            state=state,
            stream="public.orders",
            replication_key="change_seq",
            bookmark=9007199254740993,
        )

        self.assertEqual(9007199254740993, state["public.orders"]["bookmark"])
        self.assertIsInstance(state["public.orders"]["bookmark"], int)

    def test_explicit_numeric_replication_key_uses_incremental_strategy(self):
        strategy, key = _determine_strategy_and_replication_key(
            spec={
                "name": "public.orders",
                "strategy": "INCREMENTAL",
                "replication_key": "change_seq",
            },
            cols_with_types=[
                ("id", "bigint"),
                ("change_seq", "bigint"),
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
        )

        self.assertEqual("INCREMENTAL", strategy)
        self.assertEqual("change_seq", key)

    def test_numeric_boundary_query_keeps_numeric_parameter(self):
        sql, params = build_select(
            redshift_schema="public",
            table="orders",
            columns=["id", "change_seq"],
            replication_key="change_seq",
            bookmark=123456,
            inclusive_bookmark=True,
        )

        self.assertIn('"change_seq" >= %s', sql)
        self.assertEqual([123456], params)
        self.assertIsInstance(params[0], int)

if __name__ == "__main__":
    unittest.main()
