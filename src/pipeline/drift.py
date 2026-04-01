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


def extract_key_paths(obj: Any, prefix: str = "") -> list[str]:
    """
    Recursively extract all key paths from a JSON structure.

    Examples:
      {"a": 1, "b": {"c": 2}}         → ["a", "b.c"]
      {"items": [{"x": 1, "y": 2}]}   → ["items[].x", "items[].y"]

    Only structure is captured, not values. This means two responses
    with different data but the same shape produce the same hash.
    """
    paths = []

    if isinstance(obj, dict):
        for key, value in sorted(obj.items()):
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                paths.extend(extract_key_paths(value, full_key))
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Sample first element to get the shape of list items
                paths.extend(extract_key_paths(value[0], f"{full_key}[]"))
            else:
                paths.append(full_key)
    return sorted(paths)


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
        known_hashes: dict mapping endpoint → last known hash
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
            f"Previous hash: {known[:12]}... → New hash: {current_hash[:12]}..."
        )
        return True

    log.debug(f"Drift check [{api_source}]: {endpoint} structure unchanged")
    return False
