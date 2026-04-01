"""Tests for schema drift detection."""

from src.pipeline.drift import (
    build_contract_row,
    check_drift,
    extract_key_paths,
    hash_structure,
)

# ─── extract_key_paths ────────────────────────────────────────────────────────


def test_flat_dict():
    paths = extract_key_paths({"a": 1, "b": 2})
    assert paths == ["a", "b"]


def test_nested_dict():
    paths = extract_key_paths({"a": 1, "b": {"c": 2, "d": 3}})
    assert paths == ["a", "b.c", "b.d"]


def test_list_of_dicts():
    paths = extract_key_paths({"items": [{"x": 1, "y": 2}]})
    assert paths == ["items[].x", "items[].y"]


def test_newsapi_like_structure():
    data = {
        "status": "ok",
        "totalResults": 1,
        "articles": [
            {
                "source": {"id": "bbc", "name": "BBC"},
                "title": "Test",
                "author": "Jane",
            }
        ],
    }
    paths = extract_key_paths(data)
    assert "status" in paths
    assert "totalResults" in paths
    assert "articles[].title" in paths
    assert "articles[].source.id" in paths
    assert "articles[].source.name" in paths


def test_empty_dict():
    assert extract_key_paths({}) == []


def test_paths_are_sorted():
    paths = extract_key_paths({"z": 1, "a": 2, "m": 3})
    assert paths == ["a", "m", "z"]


def test_empty_list_drops_nested_structure():
    """An empty list cannot infer nested keys, so it just returns the base key."""
    paths = extract_key_paths({"articles": []})
    assert paths == ["articles"]


def test_list_of_primitives():
    """A list of strings/ints should be treated as a single structural key."""
    paths = extract_key_paths({"tags": ["tech", "ai", "software"]})
    assert paths == ["tags"]


def test_null_value():
    """A null value is treated as a valid structural leaf node."""
    paths = extract_key_paths({"title": "Test", "description": None})
    assert paths == ["description", "title"]


# ─── hash_structure ───────────────────────────────────────────────────────────


def test_hash_is_deterministic():
    data = {"a": 1, "b": {"c": 2}}
    assert hash_structure(data) == hash_structure(data)


def test_hash_ignores_values():
    """Same keys with different values should produce the same hash."""
    data1 = {"a": 1, "b": "hello"}
    data2 = {"a": 999, "b": "world"}
    assert hash_structure(data1) == hash_structure(data2)


def test_hash_differs_on_different_keys():
    data1 = {"a": 1, "b": 2}
    data2 = {"a": 1, "c": 2}
    assert hash_structure(data1) != hash_structure(data2)


def test_hash_detects_new_field():
    """Adding a field should change the hash."""
    original = {"a": 1, "b": 2}
    updated = {"a": 1, "b": 2, "new_field": 3}
    assert hash_structure(original) != hash_structure(updated)


def test_hash_detects_removed_field():
    """Removing a field should change the hash."""
    original = {"a": 1, "b": 2, "c": 3}
    updated = {"a": 1, "b": 2}
    assert hash_structure(original) != hash_structure(updated)


# ─── check_drift ──────────────────────────────────────────────────────────────


def test_first_observation_is_drift():
    """First time seeing an endpoint should count as drift (need to store contract)."""
    assert check_drift("abc123", {}, "newsapi", "/v2/everything") is True


def test_same_hash_is_not_drift():
    known = {"newsapi:/v2/everything": "abc123"}
    assert check_drift("abc123", known, "newsapi", "/v2/everything") is False


def test_different_hash_is_drift():
    known = {"newsapi:/v2/everything": "abc123"}
    assert check_drift("xyz789", known, "newsapi", "/v2/everything") is True


# ─── build_contract_row ───────────────────────────────────────────────────────


def test_build_contract_row():
    row = build_contract_row(
        api_source="newsapi",
        endpoint="/v2/everything",
        structure_hash="hash123",
        key_paths=["a", "b"],
        pipeline_run_id="run-001",
    )
    assert row["api_source"] == "newsapi"
    assert row["structure_hash"] == "hash123"
    assert row["is_current"] is True
    assert row["drift_from"] is None
    assert row["detected_by_run"] == "run-001"
