"""
Table specification loader for the Redshift connector.

Active table specifications live directly under:
    tables/<schema>.<table>.json

The file name is the table identity. JSON content only needs to contain values
that differ from the defaults in tables/_defaults.json.

Files in subdirectories (for example tables/examples/) are documentation only
and are not loaded by the connector.
"""

import json
from copy import deepcopy
from pathlib import Path


# Preferred timestamp column names for inferring replication keys.
PREFERRED_TS_COLUMN_NAMES = [
    "updated_at",
    "last_updated",
    "last_update",
    "last_modified",
    "modified_at",
    "modified_on",
    "updated_on",
    "update_time",
    "updated",
]

# Redshift data types eligible for replication-key inference.
TIMESTAMP_TYPE_NAMES = {
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamptz",
    "date",
}

CHECKPOINT_EVERY_ROWS = 50000
CHUNK_SIZE = 10000

TABLE_SPECS_DIR = Path(__file__).resolve().parent / "tables"
DEFAULTS_FILE_NAME = "_defaults.json"

_BUILTIN_DEFAULTS = {
    "primary_keys": None,
    "strategy": "AUTO",
    "replication_key": None,
    "include": [],
    "exclude": [],
    "use_chunking": False,
    "column_types": {},
    "filter": None,
    "enabled": True,
}

_ALLOWED_SPEC_KEYS = {
    "name",
    "primary_keys",
    "strategy",
    "replication_key",
    "include",
    "exclude",
    "use_chunking",
    "column_types",
    "filter",
    "enabled",
}


def _read_json(path: Path) -> dict:
    """Read one JSON object and report the source file on parse errors."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{path}: invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if not isinstance(value, dict):
        raise ValueError(f"{path}: table specification must be a JSON object.")
    return value


def _validate_string_list(value, field, source, allow_none=False):
    if value is None and allow_none:
        return
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        suffix = " or null" if allow_none else ""
        raise ValueError(f"{source}: '{field}' must be a list of non-empty strings{suffix}.")


def _validate_spec(spec: dict, source: Path, expected_name: str | None = None):
    unknown = sorted(set(spec) - _ALLOWED_SPEC_KEYS)
    if unknown:
        raise ValueError(f"{source}: unsupported key(s): {', '.join(unknown)}")

    if expected_name is not None and "name" in spec and spec["name"] != expected_name:
        raise ValueError(
            f"{source}: 'name' must match the file name ({expected_name}) "
            f"or be omitted."
        )

    strategy = spec.get("strategy")
    if strategy is not None:
        if not isinstance(strategy, str) or strategy.upper() not in {
            "AUTO",
            "FULL",
            "INCREMENTAL",
        }:
            raise ValueError(
                f"{source}: 'strategy' must be AUTO, FULL, INCREMENTAL, or null."
            )

    _validate_string_list(
        spec.get("primary_keys"), "primary_keys", source, allow_none=True
    )
    _validate_string_list(spec.get("include", []), "include", source)
    _validate_string_list(spec.get("exclude", []), "exclude", source)

    replication_key = spec.get("replication_key")
    if replication_key is not None and (
        not isinstance(replication_key, str) or not replication_key.strip()
    ):
        raise ValueError(
            f"{source}: 'replication_key' must be a non-empty string or null."
        )

    for field in ("use_chunking", "enabled"):
        if field in spec and not isinstance(spec[field], bool):
            raise ValueError(f"{source}: '{field}' must be true or false.")

    column_types = spec.get("column_types", {})
    if not isinstance(column_types, dict) or not all(
        isinstance(key, str)
        and key.strip()
        and isinstance(value, str)
        and value.strip()
        for key, value in column_types.items()
    ):
        raise ValueError(
            f"{source}: 'column_types' must be an object of non-empty string pairs."
        )

    filter_condition = spec.get("filter")
    if filter_condition is not None and not isinstance(filter_condition, dict):
        raise ValueError(f"{source}: 'filter' must be an object or null.")


def _table_name_from_path(path: Path) -> str:
    """Convert tables/schema.table.json to schema.table."""
    stem = path.stem
    if "." not in stem:
        raise ValueError(
            f"{path}: file name must be '<schema>.<table>.json'."
        )

    schema, table = stem.split(".", 1)
    if not schema.strip() or not table.strip():
        raise ValueError(
            f"{path}: file name must contain both schema and table names."
        )
    return f"{schema}.{table}"


def load_table_specs(directory: Path | str | None = None) -> list[dict]:
    """
    Load active table specifications.

    Loading rules:
    - tables/_defaults.json provides project-wide defaults.
    - only tables/*.json is scanned; subdirectories are ignored.
    - tables/<schema>.<table>.json identifies one table.
    - values in the table file override project defaults.
    - enabled=false disables that table without deleting its file.
    - primary_keys=null means discover PK metadata from Redshift.
    - primary_keys=[] explicitly means no primary key.
    - strategy=AUTO infers INCREMENTAL when a timestamp/date key is available,
      otherwise falls back to FULL.
    """
    specs_dir = Path(directory) if directory is not None else TABLE_SPECS_DIR

    defaults = deepcopy(_BUILTIN_DEFAULTS)
    defaults_path = specs_dir / DEFAULTS_FILE_NAME
    if defaults_path.exists():
        file_defaults = _read_json(defaults_path)
        _validate_spec(file_defaults, defaults_path)
        defaults.update(file_defaults)

    if not specs_dir.exists():
        return []

    loaded = []
    seen_names = set()

    for path in sorted(specs_dir.glob("*.json")):
        if path.name == DEFAULTS_FILE_NAME or path.name.startswith("_"):
            continue

        table_name = _table_name_from_path(path)
        raw_spec = _read_json(path)
        _validate_spec(raw_spec, path, expected_name=table_name)

        spec = deepcopy(defaults)
        spec.update(raw_spec)
        spec["name"] = table_name

        # Normalize strategy once so the Redshift planner gets a stable value.
        strategy = spec.get("strategy")
        spec["strategy"] = strategy.upper() if isinstance(strategy, str) else strategy

        if not spec.get("enabled", True):
            continue

        spec.pop("enabled", None)

        if table_name in seen_names:
            raise ValueError(f"{path}: duplicate table specification for {table_name}.")
        seen_names.add(table_name)
        loaded.append(spec)

    return loaded


# Imported by redshift_client.py. Loading at import time makes malformed table
# configuration fail fast before any Redshift sync starts.
TABLE_SPECS = load_table_specs()
