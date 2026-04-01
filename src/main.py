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
     e. Transform → structured + unified rows
     f. Load into BigQuery
  3. Log errors
  4. Complete the pipeline run with summary stats
  5. Send Slack notification on failure

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

import requests

from src.clients.newsapi import (
    build_error_row,
    build_raw_response_row,
    fetch_articles,
    transform_to_brand_mentions,
    transform_to_news_articles,
    validate_response,
)
from src.config import Config, load_config
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


# ─── Notifications ────────────────────────────────────────────────────────────


def send_slack_alert(config: Config, message: str) -> None:
    """Send a failure alert to Slack. Fails silently if webhook is not configured."""
    if not config.slack_webhook_url:
        log.warning("Slack webhook not configured — skipping alert")
        return

    try:
        payload = {"text": f":rotating_light: *TwoLens Pipeline Alert*\n{message}"}
        resp = requests.post(config.slack_webhook_url, json=payload, timeout=10)
        if resp.status_code != 200:
            log.error(f"Slack alert failed: HTTP {resp.status_code}")
    except Exception as e:
        log.error(f"Slack alert failed: {e}")


# ─── Pipeline Logic ──────────────────────────────────────────────────────────


def run_newsapi(
    config: Config,
    loader: BigQueryLoader,
    pipeline_run_id: str,
    known_hashes: dict[str, str],
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Run the NewsAPI pipeline for all query terms.

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
                break

            log.error(f"NewsAPI: Skipping '{term}' due to error: {result.error_message}")
            continue

        # 4. Check for schema drift
        total_results = result.raw_response.get("totalResults", 0)

        if total_results == 0:
            log.info(f"NewsAPI: No articles found for '{term}'. Skipping schema drift check.")
        else:
            drift_detected = check_drift(response_hash, known_hashes, "newsapi", "/v2/everything")

            if drift_detected:
                key_paths = extract_key_paths(result.raw_response)
                contract_row = build_contract_row(
                    "newsapi", "/v2/everything", response_hash, key_paths, pipeline_run_id
                )
                # Check if this is an update to an existing contract
                lookup_key = "newsapi:/v2/everything"
                if lookup_key in known_hashes:
                    contract_row["drift_from"] = known_hashes[lookup_key]

                if not dry_run:
                    loader.insert_api_contracts([contract_row])
                known_hashes[lookup_key] = response_hash

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

    # Track known hashes for drift detection (in production, load from api_contracts table)
    known_hashes: dict[str, str] = {}

    # ─── Run each source ──────────────────────────────────────────────
    total_errors = 0
    total_loaded = 0

    # Lens 1: NewsAPI
    news_stats = run_newsapi(config, loader, run_id, known_hashes, dry_run=args.dry_run)
    total_loaded += news_stats["mentions"]
    total_errors += news_stats["errors"]

    # Lens 2: YouTube (TODO: next module)
    youtube_records = 0

    # ─── Complete pipeline run ────────────────────────────────────────
    status = "success"
    if total_errors > 0 and total_loaded > 0:
        status = "partial"
    elif total_errors > 0 and total_loaded == 0:
        status = "failed"

    notes = (
        f"NewsAPI: {news_stats['articles']} articles -> {news_stats['mentions']} mentions. "
        f"Errors: {total_errors}"
    )

    if not args.dry_run:
        loader.complete_run(
            run_id=run_id,
            started_at=started_at,
            status=status,
            newsapi_records=news_stats["articles"],
            youtube_records=youtube_records,
            total_loaded=total_loaded,
            total_errors=total_errors,
            quota_used=0,
            notes=notes,
        )

    log.info(f"Pipeline complete | status={status} | {notes}")

    # Send alert on failure
    if status == "failed":
        send_slack_alert(config, f"Pipeline run `{run_id}` failed.\n{notes}")

    # Exit with appropriate code
    if status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
