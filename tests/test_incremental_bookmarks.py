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
    def truncate(*args, **kwargs):
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
    SNAPSHOT_ROW_ID_COLUMN,
    _build_plan,
    _checkpoint,
    _determine_strategy_and_replication_key,
    build_select,
    sync_table,
    sync_table_chunked_cursors,
    upsert_record,
)


class DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        pass


class SnapshotCursor:
    def __init__(self, rows, columns):
        self._rows = list(rows)
        self._returned = False
        self.description = [(column,) for column in columns]

    def execute(self, sql, *args, **kwargs):
        pass

    def fetchall(self):
        if self._returned:
            return []
        self._returned = True
        return list(self._rows)


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


    def test_snapshot_strategy_infers_timestamp_only_for_chunking(self):
        strategy, key = _determine_strategy_and_replication_key(
            spec={
                "name": "public.events",
                "strategy": "SNAPSHOT",
                "replication_key": None,
            },
            cols_with_types=[
                ("updated_at", "timestamp"),
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
        )

        self.assertEqual("SNAPSHOT", strategy)
        self.assertEqual("updated_at", key)

    def test_snapshot_plan_uses_synthetic_primary_key(self):
        plan = _build_plan(
            spec={
                "name": "public.events",
                "strategy": "SNAPSHOT",
                "primary_keys": [],
                "replication_key": None,
                "use_chunking": True,
            },
            table_schema="public",
            table="events",
            stream="public.events",
            table_metadata={"primary_keys": []},
            selected_cols_with_types=[
                ("updated_at", "timestamp"),
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
            auto_schema_detection=False,
        )

        self.assertEqual("SNAPSHOT", plan.strategy)
        self.assertEqual([SNAPSHOT_ROW_ID_COLUMN], plan.primary_keys)
        self.assertEqual("LONG", plan.explicit_columns[SNAPSHOT_ROW_ID_COLUMN])
        self.assertNotIn(SNAPSHOT_ROW_ID_COLUMN, plan.selected_columns)
        self.assertEqual("updated_at", plan.replication_key)

    def test_snapshot_rejects_source_column_name_collision(self):
        with self.assertRaises(ValueError):
            _build_plan(
                spec={
                    "name": "public.events",
                    "strategy": "SNAPSHOT",
                    "primary_keys": [],
                },
                table_schema="public",
                table="events",
                stream="public.events",
                table_metadata={"primary_keys": []},
                selected_cols_with_types=[
                    (SNAPSHOT_ROW_ID_COLUMN, "bigint"),
                    ("payload", "character varying"),
                ],
                enable_complete_resync=False,
                auto_schema_detection=False,
            )

    def test_snapshot_duplicate_rows_get_distinct_synthetic_keys(self):
        plan = _build_plan(
            spec={
                "name": "public.events",
                "strategy": "SNAPSHOT",
                "primary_keys": [],
            },
            table_schema="public",
            table="events",
            stream="public.events",
            table_metadata={"primary_keys": []},
            selected_cols_with_types=[
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
            auto_schema_detection=False,
        )
        cursor = SnapshotCursor(
            rows=[("same",), ("same",)],
            columns=["payload"],
        )

        emitted = []
        with patch("redshift_client.op.upsert", side_effect=lambda **kwargs: emitted.append(kwargs)):
            upsert_record(
                cursor=cursor,
                plan=plan,
                state={},
                replication_key=None,
                last_bookmark=None,
                batch_size=10000,
                seen=0,
                table_cursor="events_cursor",
                row_id_offset=10,
            )

        self.assertEqual(2, len(emitted))
        self.assertEqual(11, emitted[0]["data"][SNAPSHOT_ROW_ID_COLUMN])
        self.assertEqual(12, emitted[1]["data"][SNAPSHOT_ROW_ID_COLUMN])
        self.assertEqual("same", emitted[0]["data"]["payload"])
        self.assertEqual("same", emitted[1]["data"]["payload"])

    def test_snapshot_starts_with_truncate_and_ignores_saved_bookmark(self):
        plan = _build_plan(
            spec={
                "name": "public.events",
                "strategy": "SNAPSHOT",
                "primary_keys": [],
                "replication_key": "updated_at",
                "use_chunking": False,
            },
            table_schema="public",
            table="events",
            stream="public.events",
            table_metadata={"primary_keys": []},
            selected_cols_with_types=[
                ("updated_at", "timestamp"),
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
            auto_schema_detection=False,
        )

        with patch("redshift_client.op.truncate") as truncate, patch(
            "redshift_client.sync_table_server_side_cursor"
        ) as server_sync:
            sync_table(
                connection=object(),
                configuration={"batch_size": "10000"},
                plan=plan,
                state={
                    "public.events": {
                        "bookmark": "2026-09-29T12:00:00",
                        "replication_key": "updated_at",
                    }
                },
            )

        truncate.assert_called_once_with(table="public.events")
        self.assertIsNone(server_sync.call_args.kwargs["bookmark"])

    def test_snapshot_chunk_row_ids_continue_across_chunks(self):
        plan = _build_plan(
            spec={
                "name": "public.events",
                "strategy": "SNAPSHOT",
                "primary_keys": [],
                "replication_key": "updated_at",
                "use_chunking": True,
            },
            table_schema="public",
            table="events",
            stream="public.events",
            table_metadata={"primary_keys": []},
            selected_cols_with_types=[
                ("updated_at", "timestamp"),
                ("payload", "character varying"),
            ],
            enable_complete_resync=False,
            auto_schema_detection=False,
        )

        upper_bounds = ["2026-09-29T10:00:00", None]
        offsets = []

        def fake_upper_bound(*args, **kwargs):
            return upper_bounds.pop(0)

        def fake_upsert_record(*args, **kwargs):
            offsets.append(kwargs["row_id_offset"])
            # First chunk emits three rows, second emits two.
            count = 3 if len(offsets) == 1 else 2
            return kwargs["state"], kwargs["last_bookmark"], count

        with patch(
            "redshift_client._find_chunk_upper_bound",
            side_effect=fake_upper_bound,
        ), patch(
            "redshift_client._declare_cursor"
        ), patch(
            "redshift_client.upsert_record",
            side_effect=fake_upsert_record,
        ), patch(
            "redshift_client._checkpoint"
        ):
            sync_table_chunked_cursors(
                connection=DummyConnection(),
                plan=plan,
                state={},
                bookmark=None,
                batch_size=10000,
            )

        self.assertEqual([0, 3], offsets)

if __name__ == "__main__":
    unittest.main()
