"""
TwoLens — YouTube Client  (Lens 2: Creator & Consumer Voice)
──────────────────────────────────────────────────────────────
Fetches videos for brand-related query terms from the YouTube Data API v3.
Two-step fetch:
  1. search.list  -> find videos matching the query (100 units each)
  2. videos.list  -> get full stats for those videos (1 unit per call)

Transforms raw API responses into:
  1. youtube_videos rows   (source-specific structured layer)
  2. brand_mentions rows   (unified layer)

Free tier: 10,000 units/day.
  Per run per query: 1 search (100 units) + 1 video details (1 unit) = ~101 units
  At 3 terms × 2 runs/day = ~606 units/day -> 6% of quota
"""

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import requests
from pydantic import ValidationError

from src.clients.shared.fetch_model import FetchResult
from src.clients.youtube.models import (
    YouTubeSearchResponse,
    YouTubeVideoResponse,
)
from src.config import Config

log = logging.getLogger(__name__)

SEARCH_ENDPOINT = "youtube/v3/search"
VIDEOS_ENDPOINT = "youtube/v3/videos"


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _generate_video_id(video_id: str) -> str:
    """YouTube video IDs are already unique — just prefix for TwoLens namespace."""
    return f"yt_{video_id}"


def _parse_timestamp(ts: str | None) -> str | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        log.warning(f"Unparseable timestamp: {ts}")
        return None


# ─── Fetch ────────────────────────────────────────────────────────────────────


def fetch_search(config: Config, query_term: str) -> FetchResult:
    """
    Step 1: Search for videos matching a query term.
    Cost: 100 units per call.
    """
    params = {
        "key": config.youtube_api_key,
        "part": "snippet",
        "q": f'"{query_term}"',
        "type": "video",
        "maxResults": min(config.max_results_per_query, 50),
        "order": "date",
        "relevanceLanguage": "en",
    }

    safe_params = {k: v for k, v in params.items() if k != "key"}
    url = f"{config.youtube_base_url}/{SEARCH_ENDPOINT}"

    log.info(f"YouTube search: query='{query_term}', params={safe_params}")

    try:
        resp = requests.get(url, params=params, timeout=config.request_timeout)

        if resp.status_code == 403:
            data = resp.json()
            reason = data.get("error", {}).get("errors", [{}])[0].get("reason", "unknown")
            if reason == "quotaExceeded":
                log.error("YouTube: Daily quota exceeded")
                return FetchResult(
                    raw_response=data,
                    http_status=403,
                    is_error=True,
                    error_type="quota_exceeded",
                    error_message="YouTube API daily quota exceeded",
                )
            log.error(f"YouTube: Forbidden — {reason}")
            return FetchResult(
                raw_response=data,
                http_status=403,
                is_error=True,
                error_type="auth_failure",
                error_message=f"YouTube API key rejected: {reason}",
            )

        if resp.status_code == 400:
            log.error("YouTube: Bad request (likely invalid API key)")
            return FetchResult(
                raw_response={},
                http_status=400,
                is_error=True,
                error_type="auth_failure",
                error_message="Invalid YouTube API key",
            )

        resp.raise_for_status()
        data = resp.json()

        if "items" not in data:
            log.error(f"YouTube search: Response missing 'items'. Keys: {list(data.keys())}")
            return FetchResult(
                raw_response=data,
                http_status=resp.status_code,
                is_error=True,
                error_type="schema_drift",
                error_message=f"Missing 'items' key. Got: {list(data.keys())}",
                response_snippet=str(data)[:1000],
            )

        log.info(f"YouTube search: Got {len(data.get('items', []))} results for '{query_term}'")
        return FetchResult(raw_response=data, http_status=resp.status_code)

    except requests.exceptions.Timeout:
        log.error(f"YouTube search: Timeout after {config.request_timeout}s")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="timeout",
            error_message="Search request timed out",
        )
    except requests.exceptions.ConnectionError:
        log.error("YouTube search: Connection failed")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="http_error",
            error_message="Connection failed",
        )
    except requests.exceptions.RequestException as e:
        log.error(f"YouTube search: Request failed: {e}")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="http_error",
            error_message=str(e),
        )
    except ValueError:
        log.error("YouTube search: Response is not valid JSON")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="parse_error",
            error_message="Invalid JSON response",
        )


def fetch_video_details(config: Config, video_ids: list[str]) -> FetchResult:
    """
    Step 2: Get full details (statistics, tags, duration) for a batch of videos.
    Cost: 1 unit per call (batch of up to 50 IDs).
    """
    if not video_ids:
        return FetchResult(raw_response={"items": []}, http_status=200)

    params = {
        "key": config.youtube_api_key,
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(video_ids),
    }

    url = f"{config.youtube_base_url}/{VIDEOS_ENDPOINT}"

    log.info(f"YouTube videos: Fetching details for {len(video_ids)} videos")

    try:
        resp = requests.get(url, params=params, timeout=config.request_timeout)

        if resp.status_code == 403:
            data = resp.json()
            reason = data.get("error", {}).get("errors", [{}])[0].get("reason", "unknown")
            if reason == "quotaExceeded":
                log.error("YouTube: Daily quota exceeded on video details")
                return FetchResult(
                    raw_response=data,
                    http_status=403,
                    is_error=True,
                    error_type="quota_exceeded",
                    error_message="YouTube API daily quota exceeded",
                )
            return FetchResult(
                raw_response=data,
                http_status=403,
                is_error=True,
                error_type="auth_failure",
                error_message=f"YouTube API rejected: {reason}",
            )

        resp.raise_for_status()
        data = resp.json()

        if "items" not in data:
            return FetchResult(
                raw_response=data,
                http_status=resp.status_code,
                is_error=True,
                error_type="schema_drift",
                error_message=f"Missing 'items' key. Got: {list(data.keys())}",
                response_snippet=str(data)[:1000],
            )

        log.info(f"YouTube videos: Got details for {len(data.get('items', []))} videos")
        return FetchResult(raw_response=data, http_status=resp.status_code)

    except requests.exceptions.Timeout:
        log.error("YouTube videos: Timeout")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="timeout",
            error_message="Video details request timed out",
        )
    except requests.exceptions.RequestException as e:
        log.error(f"YouTube videos: Request failed: {e}")
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="http_error",
            error_message=str(e),
        )
    except ValueError:
        return FetchResult(
            raw_response={},
            is_error=True,
            error_type="parse_error",
            error_message="Invalid JSON response",
        )


# ─── Validate ─────────────────────────────────────────────────────────────────


def validate_search_response(
    result: FetchResult,
) -> tuple[YouTubeSearchResponse | None, list[str]]:
    """Validate the search.list response through Pydantic."""
    if result.is_error:
        return None, []

    warnings: list[str] = []

    try:
        parsed = YouTubeSearchResponse.model_validate(result.raw_response)
    except ValidationError as e:
        log.error(f"YouTube search: Validation failed: {e}")
        return None, [f"Search validation failed: {e}"]

    if parsed.extra_fields:
        msg = f"YouTube search: New top-level fields: {parsed.extra_fields}"
        log.warning(msg)
        warnings.append(msg)

    all_extra: set[str] = set()
    for item in parsed.items:
        all_extra.update(item.extra_fields)

    if all_extra:
        msg = f"YouTube search items: New fields detected across {len(parsed.items)} items: {all_extra}"
        log.info(msg)
        warnings.append(msg)

    valid = len(parsed.valid_items)
    skipped = len(parsed.items) - valid
    if skipped > 0:
        log.info(f"YouTube search: Skipped {skipped} invalid items")
    log.info(f"YouTube search: {valid} items passed validation")

    return parsed, warnings


def validate_video_response(
    result: FetchResult,
) -> tuple[YouTubeVideoResponse | None, list[str]]:
    """Validate the videos.list response through Pydantic."""
    if result.is_error:
        return None, []

    warnings: list[str] = []

    try:
        parsed = YouTubeVideoResponse.model_validate(result.raw_response)
    except ValidationError as e:
        log.error(f"YouTube videos: Validation failed: {e}")
        return None, [f"Video validation failed: {e}"]

    if parsed.extra_fields:
        msg = f"YouTube videos: New top-level fields: {parsed.extra_fields}"
        log.warning(msg)
        warnings.append(msg)

    all_extra: set[str] = set()
    for item in parsed.items:
        all_extra.update(item.extra_fields)

    if all_extra:
        msg = f"YouTube videos: New fields detected across {len(parsed.items)} items: {all_extra}"
        log.info(msg)
        warnings.append(msg)

    log.info(f"YouTube videos: {len(parsed.items)} items validated")
    return parsed, warnings


# ─── Transform ────────────────────────────────────────────────────────────────


def transform_to_youtube_videos(
    videos: YouTubeVideoResponse,
    query_term: str,
    pipeline_run_id: str,
) -> list[dict[str, Any]]:
    """Transform validated video details into youtube_videos rows."""
    now = datetime.now(UTC).isoformat()
    rows = []

    for video in videos.items:
        if not video.is_valid:
            continue

        thumbnail_url = ""
        if video.snippet.thumbnails and video.snippet.thumbnails.default:
            thumbnail_url = video.snippet.thumbnails.default.url

        rows.append(
            {
                "video_id": video.id,
                "channel_id": video.snippet.channel_id,
                "channel_title": video.snippet.channel_title,
                "title": video.snippet.title,
                "description": video.snippet.description or "",
                "published_at": _parse_timestamp(video.snippet.published_at),
                "tags": json.dumps(video.snippet.tags),
                "category_id": video.snippet.category_id,
                "view_count": video.statistics.views,
                "like_count": video.statistics.likes,
                "comment_count": video.statistics.comments,
                "duration": video.content_details.duration,
                "thumbnail_url": thumbnail_url,
                "url": f"https://www.youtube.com/watch?v={video.id}",
                "query_term": query_term,
                "captured_at": now,
                "pipeline_run_id": pipeline_run_id,
            }
        )

    log.info(f"YouTube transform: {len(rows)} video rows for '{query_term}'")
    return rows


def transform_to_brand_mentions(
    youtube_videos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Transform youtube_videos rows into unified brand_mentions rows."""
    mentions = []

    for video in youtube_videos:
        mentions.append(
            {
                "mention_id": _generate_video_id(video["video_id"]),
                "source_platform": "youtube",
                "source_record_id": video["video_id"],
                "query_term": video["query_term"],
                "title": video["title"],
                "body": video.get("description", ""),
                "author": video.get("channel_title"),
                "url": video["url"],
                "published_at": video.get("published_at"),
                "engagement_score": video.get("view_count", 0),
                "like_count": video.get("like_count", 0),
                "comment_count": video.get("comment_count", 0),
                "source_detail": video.get("channel_title", "Unknown"),
                "mention_type": "youtube_video",
                "captured_at": video["captured_at"],
                "pipeline_run_id": video["pipeline_run_id"],
            }
        )

    return mentions


# ─── Raw Storage Helpers ──────────────────────────────────────────────────────


def build_raw_response_row(
    result: FetchResult,
    endpoint: str,
    query_term: str,
    response_hash: str,
    pipeline_run_id: str,
) -> dict[str, Any]:
    """Build a row for raw_api_responses from a YouTube FetchResult."""
    return {
        "response_id": uuid.uuid4().hex,
        "api_source": "youtube",
        "endpoint": endpoint,
        "request_params": json.dumps({"q": query_term}),
        "response_body": json.dumps(result.raw_response)[:500_000],
        "response_hash": response_hash,
        "http_status": result.http_status,
        "captured_at": datetime.now(UTC).isoformat(),
        "pipeline_run_id": pipeline_run_id,
    }


def build_error_row(
    result: FetchResult,
    query_term: str,
    pipeline_run_id: str,
) -> dict[str, Any] | None:
    """If the FetchResult is an error, build an api_errors row."""
    if not result.is_error:
        return None

    return {
        "error_id": uuid.uuid4().hex,
        "pipeline_run_id": pipeline_run_id,
        "api_source": "youtube",
        "error_type": result.error_type or "unknown",
        "http_status": result.http_status,
        "error_message": result.error_message or "Unknown error",
        "request_context": json.dumps({"query_term": query_term}),
        "response_snippet": result.response_snippet or "",
        "severity": "critical"
        if result.error_type in ("auth_failure", "quota_exceeded")
        else "warning",
        "resolved": False,
        "occurred_at": datetime.now(UTC).isoformat(),
    }


# ─── Quota Tracking ──────────────────────────────────────────────────────────


def estimate_quota_used(num_search_calls: int, num_video_detail_calls: int) -> int:
    """Estimate YouTube API units consumed. search=100, videos=1 per call."""
    return (num_search_calls * 100) + (num_video_detail_calls * 1)
