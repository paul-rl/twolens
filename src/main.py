"""
TwoLens Pipeline Orchestrator
─────────────────────────────────
Entry point for the pipeline. Runs the full cycle:

  1. Start a pipeline run (BigQuery)
  2. For each query term:
     a. Fetch from each API
     b. Store raw response
     c. Check for schema drift
     d. Validate via Pydantic
     e. Transform -> structured + unified rows
     f. Load into BigQuery
  3. Log errors
  4. Complete the pipeline run with summary stats
  5. Send Slack notifications (failure, partial, drift, quota)

Usage:
  python src/main.py --trigger scheduled
  python src/main.py --query-terms "Nike,Adidas" --trigger manual
  python src/main.py --dry-run --trigger manual
"""

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from src.clients.newsapi.client import (
    build_error_row,
    build_raw_response_row,
    fetch_articles,
    transform_to_brand_mentions,
    transform_to_news_articles,
    validate_response,
)
from src.clients.youtube.client import (
    build_error_row as yt_build_error_row,
)
from src.clients.youtube.client import (
    build_raw_response_row as yt_build_raw_row,
)
from src.clients.youtube.client import (
    estimate_quota_used,
    fetch_search,
    fetch_video_details,
    transform_to_youtube_videos,
    validate_search_response,
    validate_video_response,
)
from src.clients.youtube.client import (
    transform_to_brand_mentions as yt_to_mentions,
)
from src.config import Config, load_config
from src.notifications import (
    DriftEvent,
    notify_drift_summary,
    notify_fatal_error,
    notify_pipeline_result,
    notify_quota_warning,
)
from src.pipeline.drift import (
    build_contract_row,
    check_drift,
    extract_key_paths,
    hash_structure,
)
from src.pipeline.loader import BigQueryLoader

# ─── Logging Setup ────────────────────────────────────────────────────────────

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "pipeline.log"),
    ],
)
log = logging.getLogger("twolens")


# ─── Pipeline Logic ──────────────────────────────────────────────────────────


def run_newsapi(
    config: Config,
    loader: BigQueryLoader,
    pipeline_run_id: str,
    baseline_hashes: dict[str, str],
    drift_events: list[DriftEvent],
    stored_contracts: set[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Run the NewsAPI pipeline for all query terms.

    Args:
        baseline_hashes: Read-only dict of last run's contract hashes
                         (loaded from BigQuery). Never mutated.
        stored_contracts: Shared set tracking which endpoints already had
                          a contract stored THIS run (avoids duplicate inserts).

    Returns a stats dict: {articles, mentions, errors}
    """
    stats = {"articles": 0, "mentions": 0, "errors": 0}

    for term in config.query_terms:
        log.info(f"=== NewsAPI: Processing '{term}' ===")

        # 1. Fetch
        result = fetch_articles(config, term)

        # 2. Store raw response
        response_hash = hash_structure(result.raw_response) if result.raw_response else ""
        raw_row = build_raw_response_row(result, term, response_hash, pipeline_run_id)

        if not dry_run:
            loader.insert_raw_responses([raw_row])

        # 3. Handle errors
        if result.is_error:
            error_row = build_error_row(result, term, pipeline_run_id)
            if error_row and not dry_run:
                loader.insert_api_errors([error_row])
            stats["errors"] += 1

            fatal_errors = [
                "auth_failure",
                "rate_limit",
            ]

            if result.error_type in fatal_errors:
                log.critical(
                    f"NewsAPI: Fatal error ({result.error_type}) on '{term}'. "
                    "Halting pipeline to prevent cascading failures."
                )
                notify_fatal_error(
                    config,
                    api_source="newsapi",
                    error_type=result.error_type,
                    error_message=result.error_message or "Unknown",
                    query_term=term,
                    run_id=pipeline_run_id,
                )
                break

            log.error(f"NewsAPI: Skipping '{term}' due to error: {result.error_message}")
            continue

        # 4. Check for schema drift (compare against BASELINE, not this run)
        total_results = result.raw_response.get("totalResults", 0)

        if total_results == 0:
            log.info(f"NewsAPI: No articles found for '{term}'. Skipping schema drift check.")
        else:
            if not result.raw_response.get("articles"):
                log.info(f"NewsAPI: No items found for '{term}'. Skipping search drift check.")
            else:
                lookup_key = "newsapi:/v2/everything"
                baseline_hash = baseline_hashes.get(lookup_key)
                drift_detected = check_drift(
                    response_hash, baseline_hashes, "newsapi", "/v2/everything"
                )

                if drift_detected and lookup_key not in stored_contracts:
                    key_paths = extract_key_paths(result.raw_response)
                    contract_row = build_contract_row(
                        "newsapi", "/v2/everything", response_hash, key_paths, pipeline_run_id
                    )
                    if baseline_hash is not None:
                        contract_row["drift_from"] = baseline_hash

                    if not dry_run:
                        loader.insert_api_contracts([contract_row])
                    stored_contracts.add(lookup_key)

                    drift_events.append(
                        DriftEvent(
                            api_source="newsapi",
                            endpoint="/v2/everything",
                            previous_hash=baseline_hash,
                            new_hash=response_hash,
                            key_paths=key_paths,
                        )
                    )

        # 5. Validate
        parsed, warnings = validate_response(result)
        if parsed is None:
            stats["errors"] += 1
            log.error(f"NewsAPI: Validation failed for '{term}'")
            continue

        for w in warnings:
            log.warning(w)

        # 6. Transform
        articles = transform_to_news_articles(parsed, term, pipeline_run_id)
        mentions = transform_to_brand_mentions(articles)

        # 7. Load
        article_errors = []
        if not dry_run:
            article_errors = loader.insert_news_articles(articles)
            mention_errors = loader.insert_brand_mentions(mentions)
            if article_errors or mention_errors:
                stats["errors"] += len(article_errors) + len(mention_errors)
        else:
            log.info(f"DRY RUN: Would insert {len(articles)} articles, {len(mentions)} mentions")

        stats["articles"] += len(articles) - len(article_errors)
        stats["mentions"] += len(mentions)

    return stats


def run_youtube(
    config: Config,
    loader: BigQueryLoader,
    pipeline_run_id: str,
    baseline_hashes: dict[str, str],
    drift_events: list[DriftEvent],
    stored_contracts: set[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Run the YouTube pipeline for all query terms.
    Two-step fetch: search.list → videos.list.

    Args:
        baseline_hashes: Read-only dict of last run's contract hashes.
        stored_contracts: Shared set tracking which endpoints already had
                          a contract stored THIS run.

    Returns a stats dict: {videos, mentions, errors, quota_used}
    """
    stats = {"videos": 0, "mentions": 0, "errors": 0, "quota_used": 0}

    for term in config.query_terms:
        log.info(f"=== YouTube: Processing '{term}' ===")

        # ── Step 1: Search ────────────────────────────────────────────
        search_result = fetch_search(config, term)

        # Store raw search response
        search_hash = (
            hash_structure(search_result.raw_response) if search_result.raw_response else ""
        )
        raw_row = yt_build_raw_row(
            search_result, "youtube/v3/search", term, search_hash, pipeline_run_id
        )
        if not dry_run:
            loader.insert_raw_responses([raw_row])

        # Handle search errors
        if search_result.is_error:
            error_row = yt_build_error_row(search_result, term, pipeline_run_id)
            if error_row and not dry_run:
                loader.insert_api_errors([error_row])
            stats["errors"] += 1
            fatal_errors = ["auth_failure", "quota_exceeded"]
            if search_result.error_type in fatal_errors:
                log.critical(
                    f"YouTube: Fatal error ({search_result.error_type}) on '{term}'. "
                    "Halting YouTube to prevent cascading failures."
                )
                notify_fatal_error(
                    config,
                    api_source="youtube",
                    error_type=search_result.error_type,
                    error_message=search_result.error_message or "Unknown",
                    query_term=term,
                    run_id=pipeline_run_id,
                )
                break
            log.error(
                f"YouTube: Skipping '{term}' due to search error: {search_result.error_message}"
            )
            continue

        # Drift check on search endpoint (compare against BASELINE only)
        lookup_key = "youtube:search.list"
        baseline_hash = baseline_hashes.get(lookup_key)
        drift_detected = check_drift(search_hash, baseline_hashes, "youtube", "search.list")
        if drift_detected and lookup_key not in stored_contracts:
            key_paths = extract_key_paths(search_result.raw_response)
            contract_row = build_contract_row(
                "youtube", "search.list", search_hash, key_paths, pipeline_run_id
            )
            if baseline_hash is not None:
                contract_row["drift_from"] = baseline_hash
            if not dry_run:
                loader.insert_api_contracts([contract_row])
            stored_contracts.add(lookup_key)

            drift_events.append(
                DriftEvent(
                    api_source="youtube",
                    endpoint="search.list",
                    previous_hash=baseline_hash,
                    new_hash=search_hash,
                    key_paths=key_paths,
                )
            )

        # Validate search response
        search_parsed, search_warnings = validate_search_response(search_result)
        if search_parsed is None:
            stats["errors"] += 1
            log.error(f"YouTube: Search validation failed for '{term}'")
            continue

        for w in search_warnings:
            log.warning(w)

        video_ids = search_parsed.video_ids
        if not video_ids:
            log.info(f"YouTube: No videos found for '{term}'")
            stats["quota_used"] += estimate_quota_used(1, 0)
            continue

        # ── Step 2: Video Details ─────────────────────────────────────
        details_result = fetch_video_details(config, video_ids)

        # Store raw video details response
        details_hash = (
            hash_structure(details_result.raw_response) if details_result.raw_response else ""
        )
        raw_row = yt_build_raw_row(
            details_result, "youtube/v3/videos", term, details_hash, pipeline_run_id
        )
        if not dry_run:
            loader.insert_raw_responses([raw_row])

        # Handle video details errors
        if details_result.is_error:
            error_row = yt_build_error_row(details_result, term, pipeline_run_id)
            if error_row and not dry_run:
                loader.insert_api_errors([error_row])
            stats["errors"] += 1
            if details_result.error_type in ("auth_failure", "quota_exceeded"):
                log.critical(
                    f"YouTube: Fatal error ({details_result.error_type}) on video details. "
                    "Halting YouTube."
                )
                notify_fatal_error(
                    config,
                    api_source="youtube",
                    error_type=details_result.error_type,
                    error_message=details_result.error_message or "Unknown",
                    query_term=term,
                    run_id=pipeline_run_id,
                )
                break
            log.error(f"YouTube: Skipping '{term}' video details: {details_result.error_message}")
            stats["quota_used"] += estimate_quota_used(1, 1)
            continue

        # Drift check on videos endpoint (compare against BASELINE only)
        lookup_key_vid = "youtube:videos.list"
        baseline_hash_vid = baseline_hashes.get(lookup_key_vid)
        drift_detected = check_drift(details_hash, baseline_hashes, "youtube", "videos.list")
        if drift_detected and lookup_key_vid not in stored_contracts:
            key_paths = extract_key_paths(details_result.raw_response)
            contract_row = build_contract_row(
                "youtube", "videos.list", details_hash, key_paths, pipeline_run_id
            )
            if baseline_hash_vid is not None:
                contract_row["drift_from"] = baseline_hash_vid
            if not dry_run:
                loader.insert_api_contracts([contract_row])
            stored_contracts.add(lookup_key_vid)

            drift_events.append(
                DriftEvent(
                    api_source="youtube",
                    endpoint="videos.list",
                    previous_hash=baseline_hash_vid,
                    new_hash=details_hash,
                    key_paths=key_paths,
                )
            )

        # Validate video details
        videos_parsed, video_warnings = validate_video_response(details_result)
        if videos_parsed is None:
            stats["errors"] += 1
            log.error(f"YouTube: Video details validation failed for '{term}'")
            stats["quota_used"] += estimate_quota_used(1, 1)
            continue

        for w in video_warnings:
            log.warning(w)

        # ── Transform & Load ──────────────────────────────────────────
        videos = transform_to_youtube_videos(videos_parsed, term, pipeline_run_id)
        mentions = yt_to_mentions(videos)

        video_errors = []
        if not dry_run:
            video_errors = loader.insert_youtube_videos(videos)
            mention_errors = loader.insert_brand_mentions(mentions)
            if video_errors or mention_errors:
                stats["errors"] += len(video_errors) + len(mention_errors)
        else:
            log.info(f"DRY RUN: Would insert {len(videos)} videos, {len(mentions)} mentions")

        stats["videos"] += len(videos) - len(video_errors)
        stats["mentions"] += len(mentions)
        stats["quota_used"] += estimate_quota_used(1, 1)

    return stats


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="TwoLens Brand Intelligence Pipeline")
    parser.add_argument(
        "--query-terms",
        type=str,
        default=None,
        help="Comma-separated brand names to monitor (overrides env/config)",
    )
    parser.add_argument(
        "--trigger",
        type=str,
        choices=["scheduled", "manual", "retry"],
        default="manual",
        help="What triggered this run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without writing to BigQuery",
    )
    args = parser.parse_args()

    # Load config
    config = load_config(query_terms_override=args.query_terms)
    log.info(f"TwoLens pipeline starting | terms={config.query_terms} | dry_run={args.dry_run}")

    # Initialize BigQuery
    loader = BigQueryLoader(config)

    if not args.dry_run:
        loader.ensure_tables_exist()

    # Start pipeline run
    started_at = datetime.now(UTC)
    run_id = loader.start_run(trigger_type=args.trigger) if not args.dry_run else "dry-run"

    # Load known API contracts from BigQuery so drift detection compares
    # against LAST run's structure, not an empty baseline. On dry run or
    # first-ever run, this returns an empty dict — everything will be a
    # first observation (logged, not alerted).
    if not args.dry_run:
        baseline_hashes = loader.load_known_contracts()
    else:
        baseline_hashes: dict[str, str] = {}

    # Collect drift events across both sources — notify once at the end
    drift_events: list[DriftEvent] = []

    # Track which endpoints already had a contract stored this run
    # (avoids duplicate inserts when multiple query terms see the same drift)
    stored_contracts: set[str] = set()

    # ─── Run each source ──────────────────────────────────────────────
    total_errors = 0
    total_loaded = 0

    # Lens 1: NewsAPI
    news_stats = run_newsapi(
        config,
        loader,
        run_id,
        baseline_hashes,
        drift_events,
        stored_contracts,
        dry_run=args.dry_run,
    )
    total_loaded += news_stats["mentions"]
    total_errors += news_stats["errors"]

    # Lens 2: YouTube
    yt_stats = run_youtube(
        config,
        loader,
        run_id,
        baseline_hashes,
        drift_events,
        stored_contracts,
        dry_run=args.dry_run,
    )
    total_loaded += yt_stats["mentions"]
    total_errors += yt_stats["errors"]

    # ─── Complete pipeline run ────────────────────────────────────────
    status = "success"
    if total_errors > 0 and total_loaded > 0:
        status = "partial"
    elif total_errors > 0 and total_loaded == 0:
        status = "failed"

    completed_at = datetime.now(UTC)
    duration = (completed_at - started_at).total_seconds()

    notes = (
        f"NewsAPI: {news_stats['articles']} articles -> {news_stats['mentions']} mentions. "
        f"YouTube: {yt_stats['videos']} videos -> {yt_stats['mentions']} mentions | "
        f"Errors: {total_errors} | Quota: {yt_stats['quota_used']} units"
    )

    if not args.dry_run:
        loader.complete_run(
            run_id=run_id,
            started_at=started_at,
            status=status,
            newsapi_records=news_stats["articles"],
            youtube_records=yt_stats["videos"],
            total_loaded=total_loaded,
            total_errors=total_errors,
            quota_used=yt_stats["quota_used"],
            notes=notes,
            trigger_type=args.trigger,
        )

    log.info(f"Pipeline complete | status={status} | {notes}")

    # ─── Send Slack notifications ─────────────────────────────────────

    # Check cumulative YouTube quota for the day and warn if close to limit.
    # Prior runs' usage comes from BigQuery; add this run's contribution.
    if not args.dry_run:
        prior_quota = loader.get_daily_quota_used()
    else:
        prior_quota = 0
    cumulative_quota = prior_quota + yt_stats["quota_used"]

    # Always notify on completion (success, partial, or failed)
    notify_pipeline_result(
        config,
        run_id=run_id,
        status=status,
        news_stats=news_stats,
        yt_stats=yt_stats,
        quota=cumulative_quota,
        duration_seconds=duration,
    )

    # Send ONE drift notification if any actual structural changes occurred
    # (first observations are baselines — logged locally, not alerted)
    notify_drift_summary(config, drift_events, run_id=run_id)

    notify_quota_warning(
        config,
        quota_used=cumulative_quota,
        run_id=run_id,
    )

    # Exit with appropriate code
    if status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
