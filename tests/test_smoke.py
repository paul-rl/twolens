"""Smoke tests — verify project structure and schema are in place."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_schema_file_exists():
    """schema.sql must be present at repo root."""
    assert (REPO_ROOT / "schema.sql").is_file()


def test_schema_contains_all_tables():
    """All 7 TwoLens tables must be defined in schema.sql."""
    schema = (REPO_ROOT / "schema.sql").read_text()
    expected_tables = [
        "raw_api_responses",
        "news_articles",
        "youtube_videos",
        "brand_mentions",
        "pipeline_runs",
        "api_errors",
        "api_contracts",
    ]
    for table in expected_tables:
        assert f"twolens.{table}" in schema, f"Missing table: {table}"


def test_env_example_exists():
    """.env.example must be present so new contributors can set up."""
    assert (REPO_ROOT / ".env.example").is_file()


def test_env_example_has_required_keys():
    """All required environment variables must be documented in .env.example."""
    env_example = (REPO_ROOT / ".env.example").read_text()
    required_keys = [
        "GCP_PROJECT_ID",
        "BQ_DATASET",
        "NEWSAPI_KEY",
        "YOUTUBE_API_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]
    for key in required_keys:
        assert key in env_example, f"Missing key in .env.example: {key}"
