import json
import tempfile
import unittest
from pathlib import Path

from table_specs import load_table_specs


class TableSpecLoaderTests(unittest.TestCase):
    def write_json(self, root, relative_path, value):
        path = Path(root) / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_defaults_are_merged_and_name_comes_from_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(
                tmp,
                "_defaults.json",
                {
                    "strategy": "AUTO",
                    "primary_keys": None,
                    "replication_key": None,
                    "include": [],
                    "exclude": [],
                    "use_chunking": False,
                    "column_types": {},
                    "filter": None,
                    "enabled": True,
                },
            )
            self.write_json(
                tmp,
                "sales.orders.json",
                {"replication_key": "updated_at", "use_chunking": True},
            )

            specs = load_table_specs(tmp)

            self.assertEqual(1, len(specs))
            self.assertEqual("sales.orders", specs[0]["name"])
            self.assertEqual("AUTO", specs[0]["strategy"])
            self.assertIsNone(specs[0]["primary_keys"])
            self.assertEqual("updated_at", specs[0]["replication_key"])
            self.assertTrue(specs[0]["use_chunking"])

    def test_explicit_empty_primary_key_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(tmp, "sales.events.json", {"primary_keys": []})

            specs = load_table_specs(tmp)

            self.assertEqual([], specs[0]["primary_keys"])

    def test_disabled_table_is_not_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(tmp, "sales.orders.json", {"enabled": False})

            self.assertEqual([], load_table_specs(tmp))

    def test_subdirectories_are_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(
                tmp,
                "examples/example.pk_timestamp.json",
                {"strategy": "AUTO"},
            )

            self.assertEqual([], load_table_specs(tmp))

    def test_snapshot_strategy_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(
                tmp,
                "sales.snapshot_source.json",
                {"strategy": "SNAPSHOT", "primary_keys": []},
            )

            specs = load_table_specs(tmp)

            self.assertEqual("SNAPSHOT", specs[0]["strategy"])

    def test_name_must_match_filename_when_supplied(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.write_json(
                tmp,
                "sales.orders.json",
                {"name": "other.table"},
            )

            with self.assertRaises(ValueError):
                load_table_specs(tmp)


if __name__ == "__main__":
    unittest.main()
