"""
TwoLens NewsAPI Client  (Lens 1: Media Coverage)
────────────────────────────────────────────────────
Fetches news articles for brand-related query terms from NewsAPI's
/v2/everything endpoint. Transforms raw API responses into:
  1. news_articles rows  (source-specific structured layer)
  2. brand_mentions rows (unified layer)

Validation is handled by Pydantic models (newsapi_models.py). Articles
are validated individually so one malformed article doesn't kill the batch.
Extra fields in the API response are captured for drift detection.

Free tier limits: 100 requests/day, 24h article delay, content truncated.
"""

import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import requests
from pydantic import ValidationError

from src.clients.newsapi_models import FetchResult, NewsApiResponse
from src.config import Config

log = logging.getLogger(__name__)

ENDPOINT = "/v2/everything"


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _generate_article_id(source_name: str, published_at: str, title: str) -> str:
    """Deterministic ID from article attributes. Same article = same hash."""
    raw = f"{source_name}|{published_at}|{title}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ─── Fetch ────────────────────────────────────────────────────────────────────


def fetch_articles(config: Config, query_term: str) -> dict[str, Any]:
    """
    Fetch articles from NewsAPI for a single query term.

    Returns the full raw response dict (for drift detection and raw storage),
    or an error-shaped dict if the request fails.
    """
    params = {
        "apiKey": config.newsapi_key,
        "q": f'"{query_term}"',
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": min(config.max_results_per_query, 100),
    }

    safe_params = {k: v for k, v in params.items() if k != "apiKey"}
    url = f"{config.newsapi_base_url}{ENDPOINT}"

    log.info(f"NewsAPI fetch: query='{query_term}', params={safe_params}")

    try:
        resp = requests.get(url, params=params, timeout=config.request_timeout)

        if resp.status_code == 401:
            log.error("NewsAPI: API key is invalid or revoked")
            return FetchResult(
                raw_response={},
                http_status=401,
                is_error=True,
                error_type="auth_failure",
                error_message="Invalid API key",
            )

        if resp.status_code == 429:
            log.error("NewsAPI: Rate limit exceeded")
            return FetchResult(
                raw_response={},
                http_status=429,
                is_error=True,
                error_type="rate_limit",
                error_message="Rate limit exceeded",
            )

        resp.raise_for_status()
        data = resp.json()

        # Validate expected top-level structure
        if "articles" not in data:
            log.error(f"NewsAPI: Response missing 'articles' key. Keys: {list(data.keys())}")
            return FetchResult(
                raw_response=data,
                http_status=resp.status_code,
                is_error=True,
                error_type="schema_drift",
                error_message=f"Missing 'articles' key. Got keys: {list(data.keys())}",
                response_snippet=str(data)[:1000],
            )

        log.info(f"NewsAPI: Got {len(data.get('articles', []))} articles for '{query_term}'")
        return FetchResult(raw_response=data, http_status=resp.status_code)

    except requests.exceptions.Timeout:
        log.error(f"NewsAPI: Timeout after {config.request_timeout}s")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="timeout",
            error_message="Request timed out",
        )

    except requests.exceptions.ConnectionError:
        log.error("NewsAPI: Connection failed")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="http_error",
            error_message="Connection failed",
        )

    except requests.exceptions.RequestException as e:
        log.error(f"NewsAPI: Request failed: {e}")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="http_error",
            error_message=str(e),
        )

    except ValueError:
        log.error("NewsAPI: Response is not valid JSON")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="parse_error",
            error_message="Invalid JSON response",
        )


# ─── Validate ─────────────────────────────────────────────────────────────────


def validate_response(result: FetchResult) -> tuple[NewsApiResponse | None, list[str]]:
    """
    Validate a raw API response through the Pydantic model.

    Returns:
        (parsed_response, warnings) parsed_response is None if top-level
        validation fails. Warnings list individual article-level issues.
    """
    if result.is_error:
        return None, []

    warnings: list[str] = []

    try:
        parsed = NewsApiResponse.model_validate(result.raw_response)
    except ValidationError as e:
        log.error(f"NewsAPI: Top-level response validation failed: {e}")
        return None, [f"Response validation failed: {e}"]

    # Log drift signals: unexpected top-level fields
    if parsed.extra_fields:
        msg = f"NewsAPI: New top-level fields detected: {parsed.extra_fields}"
        log.warning(msg)
        warnings.append(msg)

    # Validate articles individually: log bad ones, keep good ones
    valid_count = 0
    skipped_count = 0
    for i, article in enumerate(parsed.articles):
        if article.extra_fields:
            msg = f"Article [{i}]: New fields detected: {article.extra_fields}"
            log.info(msg)
            warnings.append(msg)

        if not article.is_valid:
            skipped_count += 1
        else:
            valid_count += 1

    if skipped_count > 0:
        log.info(f"NewsAPI: Skipped {skipped_count} invalid articles (no title or '[Removed]')")

    log.info(f"NewsAPI: {valid_count} articles passed validation")
    return parsed, warnings


# ─── Transform ────────────────────────────────────────────────────────────────


def transform_to_news_articles(
    parsed: NewsApiResponse,
    query_term: str,
    pipeline_run_id: str,
) -> list[dict[str, Any]]:
    """
    Transform validated NewsApiResponse into news_articles rows.
    Only processes articles that passed Pydantic validation and is_valid check.
    """
    now = datetime.now(UTC).isoformat()
    rows = []

    for article in parsed.valid_articles:
        source_name = article.source.name
        published_at = article.published_at or ""

        rows.append(
            {
                "article_id": _generate_article_id(source_name, published_at, article.title),
                "source_name": source_name,
                "source_id": article.source.id,
                "author": article.author,
                "title": article.title,
                "description": article.description or "",
                "content": article.content or "",
                "url": str(article.url) if article.url else "",
                "image_url": str(article.url_to_image) if article.url_to_image else "",
                "published_at": article.published_at.isoformat() if article.published_at else None,
                "query_term": query_term,
                "captured_at": now,
                "pipeline_run_id": pipeline_run_id,
            }
        )

    log.info(f"NewsAPI transform: {len(rows)} rows for '{query_term}'")
    return rows


def transform_to_brand_mentions(
    news_articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Transform news_articles rows into unified brand_mentions rows.
    """
    mentions = []

    for article in news_articles:
        mentions.append(
            {
                "mention_id": f"news_{article['article_id']}",
                "source_platform": "newsapi",
                "source_record_id": article["article_id"],
                "query_term": article["query_term"],
                "title": article["title"],
                "body": article.get("content") or article.get("description") or "",
                "author": article.get("author"),
                "url": article.get("url"),
                "published_at": article.get("published_at"),
                "engagement_score": None,
                "like_count": None,
                "comment_count": None,
                "source_detail": article.get("source_name", "Unknown"),
                "mention_type": "news_article",
                "captured_at": article["captured_at"],
                "pipeline_run_id": article["pipeline_run_id"],
            }
        )

    return mentions


# ─── Raw Storage Helper ──────────────────────────────────────────────────────


def build_raw_response_row(
    result: FetchResult,
    query_term: str,
    response_hash: str,
    pipeline_run_id: str,
) -> dict[str, Any]:
    """Build a row for the raw_api_responses table."""
    return {
        "response_id": uuid.uuid4().hex,
        "api_source": "newsapi",
        "endpoint": ENDPOINT,
        "request_params": json.dumps(
            {"q": f"{query_term}", "language": "en", "sortBy": "publishedAt"}
        ),
        "response_body": json.dumps(result.raw_response)[:500_000],
        "response_hash": response_hash,
        "http_status": result.http_status,
        "captured_at": datetime.now(UTC).isoformat(),
        "pipeline_run_id": pipeline_run_id,
    }


# ─── Error Row Helper ────────────────────────────────────────────────────────


def build_error_row(
    result: FetchResult,
    query_term: str,
    pipeline_run_id: str,
) -> dict[str, Any] | None:
    """If the raw response is an error, build an api_errors row. Otherwise None."""
    if not result.is_error:
        return None

    return {
        "error_id": uuid.uuid4().hex,
        "pipeline_run_id": pipeline_run_id,
        "api_source": "newsapi",
        "error_type": result.error_type,
        "http_status": result.http_status,
        "error_message": result.error_message,
        "request_context": json.dumps({"query_term": query_term}),
        "response_snippet": result.response_snippet,
        "severity": "critical" if result.error_type == "auth_failure" else "warning",
        "resolved": False,
        "occurred_at": datetime.now(UTC).isoformat(),
    }
