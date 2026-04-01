"""
TwoLens — Schema Drift Detection
──────────────────────────────────
Hashes the structure of API responses (key paths, not values) and compares
against previously observed contracts. When a new structure is detected,
it's logged as a drift event.

This is the system that turns "API resilience" from a coding pattern
into a monitored, auditable system.
"""

import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)


DRIFT_IGNORE_KEYS = {"nextPageToken", "prevPageToken", "etag", "regionCode"}


def extract_key_paths(obj: Any, prefix: str = "", ignore: set[str] | None = None) -> list[str]:
    """
    Recursively extract all key paths from a JSON structure.

    For lists of dicts, unions the keys across ALL items — not just the
    first one. This is critical for APIs like YouTube where optional fields
    (tags, liveBroadcastContent, etc.) appear in some items but not others.
    Sampling only items[0] would produce a different hash depending on which
    item happens to be first, causing false drift on every run.

    Args:
        obj: The JSON object to extract paths from.
        prefix: Current key path prefix (used in recursion).
        ignore: Set of top-level or leaf key names to exclude from paths.
                Used to filter out fields that vary between requests without
                representing actual schema changes (e.g. pagination tokens).
    """
    if ignore is None:
        ignore = DRIFT_IGNORE_KEYS

    paths = []

    if isinstance(obj, dict):
        for key, value in sorted(obj.items()):
            if key in ignore:
                continue
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                paths.extend(extract_key_paths(value, full_key, ignore))
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Union keys across ALL items in the list, not just [0].
                # This produces a stable "superset schema" that doesn't
                # change when optional fields appear or disappear per item.
                merged = _merge_list_items(value)
                paths.extend(extract_key_paths(merged, f"{full_key}[]", ignore))
            else:
                paths.append(full_key)
    return sorted(paths)


def _merge_list_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Merge a list of dicts into one dict that contains every key seen
    in any item. For nested dicts, recurse. For conflicting types,
    prefer dict > list > scalar so we capture the deepest structure.

    Example:
        [{"a": 1, "b": {"x": 1}}, {"a": 2, "c": 3, "b": {"x": 1, "y": 2}}]
        → {"a": 1, "b": {"x": 1, "y": 2}, "c": 3}
    """
    merged: dict[str, Any] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if key not in merged:
                merged[key] = value
            elif isinstance(value, dict) and isinstance(merged[key], dict):
                # Both are dicts — recurse to union nested keys
                merged[key] = _merge_list_items([merged[key], value])
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Both are lists of dicts — merge the sublists
                existing = merged[key] if isinstance(merged[key], list) else []
                merged[key] = existing + value
            # else: keep the existing value (we only care about structure, not values)

    return merged


def hash_structure(obj: dict[str, Any]) -> str:
    """SHA-256 hash of the sorted key paths. Structure identity, not content."""
    paths = extract_key_paths(obj)
    raw = json.dumps(paths)
    return hashlib.sha256(raw.encode()).hexdigest()


def build_contract_row(
    api_source: str,
    endpoint: str,
    structure_hash: str,
    key_paths: list[str],
    pipeline_run_id: str,
) -> dict[str, Any]:
    """Build a row for the api_contracts table."""
    now = datetime.now(UTC).isoformat()
    return {
        "contract_id": uuid.uuid4().hex,
        "api_source": api_source,
        "endpoint": endpoint,
        "structure_hash": structure_hash,
        "structure_keys": json.dumps(key_paths),
        "first_seen_at": now,
        "last_seen_at": now,
        "is_current": True,
        "drift_from": None,  # set by caller if a previous contract exists
        "detected_by_run": pipeline_run_id,
    }


def check_drift(
    current_hash: str,
    known_hashes: dict[str, str],
    api_source: str,
    endpoint: str,
) -> bool:
    """
    Compare the current response hash against known contracts.

    Args:
        current_hash: SHA-256 of the current response structure
        known_hashes: dict mapping endpoint-> last known hash
        api_source: for logging
        endpoint: for logging and lookup

    Returns:
        True if drift was detected (new or changed structure), False if stable.
    """
    lookup_key = f"{api_source}:{endpoint}"
    known = known_hashes.get(lookup_key)

    if known is None:
        log.info(f"Drift check [{api_source}]: First observation of {endpoint}")
        return True  # first time seeing this endpoint — store the contract

    if current_hash != known:
        log.warning(
            f"Drift check [{api_source}]: Structure changed for {endpoint}! "
            f"Previous hash: {known[:12]}... -> New hash: {current_hash[:12]}..."
        )
        return True

    log.debug(f"Drift check [{api_source}]: {endpoint} structure unchanged")
    return False
