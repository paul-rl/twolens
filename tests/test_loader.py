"""Tests for BigQuery loader — focused on load_known_contracts."""

from unittest.mock import MagicMock, patch

import pytest

from src.config import Config


@pytest.fixture
def config():
    return Config(gcp_project_id="test-project", bq_dataset="twolens")


# ─── load_known_contracts ─────────────────────────────────────────────────────


@patch("src.pipeline.loader.bigquery.Client")
def test_load_known_contracts_success(mock_client_class, config):
    """Should return a dict mapping 'source:endpoint' -> hash."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Simulate BigQuery returning 2 contract rows
    mock_row_1 = MagicMock()
    mock_row_1.api_source = "newsapi"
    mock_row_1.endpoint = "/v2/everything"
    mock_row_1.structure_hash = "hash_news_abc"

    mock_row_2 = MagicMock()
    mock_row_2.api_source = "youtube"
    mock_row_2.endpoint = "search.list"
    mock_row_2.structure_hash = "hash_yt_def"

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [mock_row_1, mock_row_2]
    mock_client.query.return_value = mock_query_job

    loader = BigQueryLoader(config)
    known = loader.load_known_contracts()

    assert known == {
        "newsapi:/v2/everything": "hash_news_abc",
        "youtube:search.list": "hash_yt_def",
    }

    # Verify the query filters on is_current = TRUE
    query_arg = mock_client.query.call_args[0][0]
    assert "is_current = TRUE" in query_arg


@patch("src.pipeline.loader.bigquery.Client")
def test_load_known_contracts_empty_table(mock_client_class, config):
    """Empty table should return empty dict (first-ever run)."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    loader = BigQueryLoader(config)
    known = loader.load_known_contracts()

    assert known == {}


@patch("src.pipeline.loader.bigquery.Client")
def test_load_known_contracts_table_not_found(mock_client_class, config):
    """If the table doesn't exist yet, return empty dict (don't crash)."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.query.side_effect = Exception("Not found: Table test-project.twolens.api_contracts")

    loader = BigQueryLoader(config)
    known = loader.load_known_contracts()

    assert known == {}


# ─── get_daily_quota_used ─────────────────────────────────────────────────────


@patch("src.pipeline.loader.bigquery.Client")
def test_get_daily_quota_returns_sum(mock_client_class, config):
    """Should return total quota_used across all runs today."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_row = MagicMock()
    mock_row.total = 606  # two runs at 303 each

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    loader = BigQueryLoader(config)
    total = loader.get_daily_quota_used()

    assert total == 606

    query_arg = mock_client.query.call_args[0][0]
    assert "CURRENT_DATE()" in query_arg
    assert "status != 'running'" in query_arg


@patch("src.pipeline.loader.bigquery.Client")
def test_get_daily_quota_no_runs_today(mock_client_class, config):
    """No runs today should return 0."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_row = MagicMock()
    mock_row.total = 0

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    loader = BigQueryLoader(config)
    assert loader.get_daily_quota_used() == 0


@patch("src.pipeline.loader.bigquery.Client")
def test_get_daily_quota_table_missing(mock_client_class, config):
    """If pipeline_runs doesn't exist yet, return 0 (don't crash)."""
    from src.pipeline.loader import BigQueryLoader

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_client.query.side_effect = Exception("Not found: Table test-project.twolens.pipeline_runs")

    loader = BigQueryLoader(config)
    assert loader.get_daily_quota_used() == 0
