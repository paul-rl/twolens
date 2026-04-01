"""
TwoLens — Slack Notifications
──────────────────────────────
Centralized alert system for pipeline events. Sends structured Slack
messages via incoming webhook for:

  - Pipeline failures and partial successes
  - Schema drift detection (batched per run, not per query term)
  - YouTube quota warnings
  - Pipeline success summaries

Design:
  - Every public function fails silently (log + return False).
    Notifications are important but not worth crashing the pipeline over.
  - Messages use Slack Block Kit for readable formatting.
  - All functions accept a Config object so the webhook URL comes from
    one place (src/config.py), not from os.environ scattered around.
  - Drift notifications are batched: one message per run, not per endpoint
    per query term. First observations (baseline contracts) are logged
    locally but don't fire Slack alerts — only actual structural changes
    from a known contract trigger a notification.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import requests

from src.config import Config

log = logging.getLogger(__name__)

# YouTube free tier: 10,000 units/day
YOUTUBE_QUOTA_LIMIT = 10_000
YOUTUBE_QUOTA_WARN_PCT = 0.80  # alert at 80% usage


@dataclass
class DriftEvent:
    """A single schema drift observation collected during a pipeline run."""

    api_source: str
    endpoint: str
    previous_hash: str | None  # None = first observation (baseline)
    new_hash: str
    key_paths: list[str] = field(default_factory=list)

    @property
    def is_first_observation(self) -> bool:
        """First observations are baselines, not actionable drift."""
        return self.previous_hash is None

    @property
    def is_actual_change(self) -> bool:
        """True only when a known contract changed — the kind worth alerting on."""
        return self.previous_hash is not None


# ─── Core Sender ──────────────────────────────────────────────────────────────


def _send(config: Config, payload: dict[str, Any]) -> bool:
    """
    Post a payload to the configured Slack webhook.

    Returns True on success, False on any failure.
    Never raises — alerts should not kill the pipeline.
    """
    if not config.slack_webhook_url:
        log.warning("Slack webhook not configured — skipping notification")
        return False

    try:
        resp = requests.post(
            config.slack_webhook_url,
            json=payload,
            timeout=10,
        )
        if resp.status_code != 200:
            log.error(f"Slack webhook returned HTTP {resp.status_code}: {resp.text}")
            return False

        log.info("Slack notification sent successfully")
        return True

    except requests.exceptions.Timeout:
        log.error("Slack notification timed out")
        return False
    except requests.exceptions.RequestException as e:
        log.error(f"Slack notification failed: {e}")
        return False


# ─── Message Builders ─────────────────────────────────────────────────────────


def _header_block(text: str) -> dict[str, Any]:
    return {"type": "header", "text": {"type": "plain_text", "text": text, "emoji": True}}


def _section_block(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _divider() -> dict[str, Any]:
    return {"type": "divider"}


def _fields_block(fields: list[str]) -> dict[str, Any]:
    return {
        "type": "section",
        "fields": [{"type": "mrkdwn", "text": f} for f in fields],
    }


def _context_block(text: str) -> dict[str, Any]:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


# ─── Public Alert Functions ───────────────────────────────────────────────────


def notify_pipeline_result(
    config: Config,
    run_id: str,
    status: str,
    news_stats: dict[str, int],
    yt_stats: dict[str, int],
    duration_seconds: float | None = None,
) -> bool:
    """
    Send a summary notification after pipeline completion.
    Fires on: success, partial, or failed status.

    Returns True if notification was sent successfully.
    """
    emoji_map = {
        "success": ":large_green_circle:",
        "partial": ":large_yellow_circle:",
        "failed": ":red_circle:",
    }
    emoji = emoji_map.get(status, ":question:")
    title = f"{emoji} TwoLens Pipeline — {status.upper()}"

    total_records = news_stats.get("mentions", 0) + yt_stats.get("mentions", 0)
    total_errors = news_stats.get("errors", 0) + yt_stats.get("errors", 0)

    fields = [
        f"*Status:*\n{status.capitalize()}",
        f"*Run ID:*\n`{run_id[:12]}...`",
        f"*NewsAPI:*\n{news_stats.get('articles', 0)} articles → {news_stats.get('mentions', 0)} mentions",
        f"*YouTube:*\n{yt_stats.get('videos', 0)} videos → {yt_stats.get('mentions', 0)} mentions",
        f"*Total Loaded:*\n{total_records} records",
        f"*Errors:*\n{total_errors}",
    ]

    blocks: list[dict[str, Any]] = [
        _header_block(title),
        _divider(),
        _fields_block(fields),
    ]

    # Add quota info if YouTube ran
    quota_used = yt_stats.get("quota_used", 0)
    if quota_used > 0:
        quota_pct = (quota_used / YOUTUBE_QUOTA_LIMIT) * 100
        blocks.append(
            _section_block(
                f"*YouTube Quota:* {quota_used:,} / {YOUTUBE_QUOTA_LIMIT:,} units "
                f"({quota_pct:.1f}% used today)"
            )
        )

    # Duration
    if duration_seconds is not None:
        blocks.append(_context_block(f"Completed in {duration_seconds:.1f}s"))

    blocks.append(
        _context_block(f"Pipeline run at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    )

    return _send(config, {"blocks": blocks})


def notify_drift_summary(
    config: Config,
    drift_events: list[DriftEvent],
    run_id: str = "",
) -> bool:
    """
    Send ONE batched drift notification for the entire pipeline run.

    Rules:
      - First observations (baselines) are skipped — they're not actionable.
      - Multiple drift events on the same endpoint are deduplicated (we only
        care that search.list changed, not that it changed 3 times across
        3 query terms within the same run).
      - If no actual changes occurred, no notification is sent.

    Returns True if a notification was sent.
    """
    # Filter to actual changes only (not first observations)
    actual_changes = [e for e in drift_events if e.is_actual_change]

    if not actual_changes:
        if drift_events:
            log.info(
                f"Drift: {len(drift_events)} first observation(s) logged — "
                "no Slack alert needed for baseline contracts"
            )
        return False

    # Deduplicate by (api_source, endpoint) — keep the latest per endpoint
    seen: dict[str, DriftEvent] = {}
    for event in actual_changes:
        key = f"{event.api_source}:{event.endpoint}"
        seen[key] = event  # last write wins

    unique_changes = list(seen.values())

    title = ":warning: Schema Drift Detected"
    if len(unique_changes) == 1:
        title += f" — {unique_changes[0].api_source}/{unique_changes[0].endpoint}"

    blocks: list[dict[str, Any]] = [
        _header_block(title),
        _divider(),
    ]

    for event in unique_changes:
        change_text = (
            f"*`{event.api_source}` — `{event.endpoint}`*\n"
            f"Previous: `{event.previous_hash[:12]}...` → "
            f"New: `{event.new_hash[:12]}...`"
        )
        blocks.append(_section_block(change_text))

        if event.key_paths:
            # Show a compact sample of the new structure
            sample = ", ".join(f"`{p}`" for p in event.key_paths[:8])
            suffix = f" (+{len(event.key_paths) - 8} more)" if len(event.key_paths) > 8 else ""
            blocks.append(_context_block(f"Key paths: {sample}{suffix}"))

    blocks.append(_divider())
    blocks.append(
        _section_block(
            f"*{len(unique_changes)} endpoint(s)* changed structure this run. "
            "Raw responses are preserved for review."
        )
    )

    if run_id:
        blocks.append(_context_block(f"Detected by run `{run_id[:12]}...`"))

    return _send(config, {"blocks": blocks})


def notify_quota_warning(
    config: Config,
    quota_used: int,
    run_id: str = "",
) -> bool:
    """
    Alert when YouTube API quota usage exceeds the warning threshold.

    Free tier = 10,000 units/day. At 80% we alert so the team can
    decide whether to skip the next scheduled run.
    """
    pct = (quota_used / YOUTUBE_QUOTA_LIMIT) * 100

    if pct < YOUTUBE_QUOTA_WARN_PCT * 100:
        return False  # not worth alerting yet

    blocks: list[dict[str, Any]] = [
        _header_block(":fuel: YouTube API Quota Warning"),
        _divider(),
        _section_block(
            f"*{quota_used:,} / {YOUTUBE_QUOTA_LIMIT:,} units used today* ({pct:.1f}%)\n\n"
            f"Remaining: {YOUTUBE_QUOTA_LIMIT - quota_used:,} units. "
            "Consider skipping the next scheduled run to avoid exhausting the daily quota."
        ),
    ]

    if run_id:
        blocks.append(_context_block(f"Run `{run_id[:12]}...`"))

    return _send(config, {"blocks": blocks})


def notify_fatal_error(
    config: Config,
    api_source: str,
    error_type: str,
    error_message: str,
    query_term: str = "",
    run_id: str = "",
) -> bool:
    """
    Immediate alert for fatal errors that halt part of the pipeline.

    Fatal errors: auth_failure, quota_exceeded, repeated timeouts.
    These need human attention — either a key rotated, a quota is
    exhausted, or a service is down.
    """
    blocks: list[dict[str, Any]] = [
        _header_block(":rotating_light: Fatal Pipeline Error"),
        _divider(),
        _fields_block(
            [
                f"*Source:*\n{api_source}",
                f"*Error Type:*\n`{error_type}`",
                f"*Query Term:*\n{query_term or 'N/A'}",
                f"*Message:*\n{error_message}",
            ]
        ),
        _section_block(
            "Pipeline halted for this source to prevent cascading failures. "
            "Check the error and re-run manually if needed."
        ),
    ]

    if run_id:
        blocks.append(_context_block(f"Run `{run_id[:12]}...`"))

    return _send(config, {"blocks": blocks})
