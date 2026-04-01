"""
TwoLens Configuration
─────────────────────
Centralizes all environment variables, constants, and pipeline settings.
Single source of truth, no module reads os.environ directly.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration loaded from environment."""

    # GCP / BigQuery
    gcp_project_id: str = ""
    bq_dataset: str = "twolens"

    # API keys
    newsapi_key: str = ""
    youtube_api_key: str = ""

    # Notifications
    slack_webhook_url: str = ""

    # Pipeline settings
    query_terms: list[str] = field(default_factory=lambda: ["Avenue Z", "Anthropic", "OpenAI"])
    max_results_per_query: int = 25

    # API endpoints
    newsapi_base_url: str = "https://newsapi.org"
    youtube_base_url: str = "https://www.googleapis.com/youtube"

    # Timeouts (seconds)
    request_timeout: int = 15


def load_config(query_terms_override: str | None = None) -> Config:
    """Build config from environment variables with optional overrides."""
    terms = os.environ.get("QUERY_TERMS", "Avenue Z,Anthropic,OpenAI")
    if query_terms_override:
        terms = query_terms_override

    return Config(
        gcp_project_id=os.environ.get("GCP_PROJECT_ID", ""),
        bq_dataset=os.environ.get("BQ_DATASET", "twolens"),
        newsapi_key=os.environ.get("NEWSAPI_KEY", ""),
        youtube_api_key=os.environ.get("YOUTUBE_API_KEY", ""),
        slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL", ""),
        query_terms=[t.strip() for t in terms.split(",") if t.strip()],
        max_results_per_query=int(os.environ.get("MAX_RESULTS_PER_QUERY", "25")),
    )
