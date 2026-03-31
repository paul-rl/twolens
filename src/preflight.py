"""
TwoLens Preflight Checks
─────────────────────────
Validates that all external dependencies are reachable before the pipeline
runs. Catches environment misconfigs, rotated keys, and service outages
BEFORE we waste a pipeline run on them.

Exit codes:
  0 — all checks passed
  1 — one or more critical checks failed (pipeline should not run)
"""

import logging
import os
import sys
from collections.abc import Callable
from typing import Any, TypeVar


import requests
from requests import Response

# Type alias for the functions being decorated
F = TypeVar("F", bound=Callable[..., Any])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PREFLIGHT] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CHECKS_PASSED = 0
CHECKS_FAILED = 0


def check(name: str, critical: bool = True) -> Callable[[F], Callable[[], None]]:
    """Decorator that wraps a check function with pass/fail logging."""

    def decorator(fn: F) -> Callable[[], None]:
        def wrapper() -> None:
            global CHECKS_PASSED, CHECKS_FAILED
            try:
                fn()
                log.info(f"✓  {name}")
                CHECKS_PASSED += 1
            except Exception as e:
                level = "CRITICAL" if critical else "WARNING"
                log.error(f"✗  {name} [{level}]: {e}")
                if critical:
                    CHECKS_FAILED += 1

        return wrapper

    return decorator


# ─── Environment Variables ────────────────────────────────────────────────────


@check("Required env vars are set", critical=True)
def check_env_vars() -> None:
    required = ["GCP_PROJECT_ID", "BQ_DATASET", "NEWSAPI_KEY", "YOUTUBE_API_KEY"]
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise OSError(f"Missing environment variables: {', '.join(missing)}")


# ─── NewsAPI ──────────────────────────────────────────────────────────────────


@check("NewsAPI key is valid", critical=True)
def check_newsapi() -> None:
    key: str = os.environ.get("NEWSAPI_KEY", "")
    resp: Response = requests.get(
        "https://newsapi.org/v2/top-headlines",
        params={"apiKey": key, "country": "us", "pageSize": 1},
        timeout=10,
    )
    if resp.status_code == 401:
        raise PermissionError("NewsAPI key is invalid or revoked")
    if resp.status_code == 429:
        raise RuntimeError("NewsAPI rate limit already exhausted for today")
    resp.raise_for_status()


# ─── YouTube Data API ─────────────────────────────────────────────────────────


@check("YouTube API key is valid", critical=True)
def check_youtube() -> None:
    key: str = os.environ.get("YOUTUBE_API_KEY", "")
    resp: Response = requests.get(
        "https://www.googleapis.com/youtube/v3/search",
        params={"key": key, "part": "snippet", "q": "test", "maxResults": 1, "type": "video"},
        timeout=10,
    )
    if resp.status_code == 403:
        data: dict[str, Any] = resp.json()
        reason: str = data.get("error", {}).get("errors", [{}])[0].get("reason", "unknown")
        if reason == "quotaExceeded":
            raise RuntimeError("YouTube API daily quota already exhausted")
        raise PermissionError(f"YouTube API key rejected: {reason}")
    if resp.status_code == 400:
        raise PermissionError("YouTube API key is invalid")
    resp.raise_for_status()


# ─── BigQuery ─────────────────────────────────────────────────────────────────


@check("BigQuery dataset is reachable", critical=True)
def check_bigquery() -> None:
    from google.cloud import bigquery

    project = os.environ.get("GCP_PROJECT_ID", "")
    dataset = os.environ.get("BQ_DATASET", "twolens")
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset)
    client.get_dataset(dataset_ref)


# ─── Slack Webhook (non-critical) ──────────────────────────────────────────


@check("Slack webhook is reachable", critical=False)
def check_slack() -> None:
    url: str = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        raise RuntimeError("SLACK_WEBHOOK_URL not set — alerts will be skipped")
    # HEAD request to verify the webhook URL is valid without posting a message
    resp: Response = requests.head(url, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"Slack webhook returned HTTP {resp.status_code}")


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("Starting preflight checks...")
    log.info("─" * 50)

    check_env_vars()
    check_newsapi()
    check_youtube()
    check_bigquery()
    check_slack()

    log.info("─" * 50)
    log.info(f"Results: {CHECKS_PASSED} passed, {CHECKS_FAILED} failed")

    if CHECKS_FAILED > 0:
        log.error("Preflight FAILED — pipeline will not run.")
        sys.exit(1)

    log.info("Preflight PASSED — pipeline is clear to run.")
    sys.exit(0)


if __name__ == "__main__":
    main()
