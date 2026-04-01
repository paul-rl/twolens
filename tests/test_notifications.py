"""Tests for the Slack notification module."""

from unittest.mock import MagicMock, patch

import pytest

from src.config import Config
from src.notifications import (
    DriftEvent,
    notify_drift_summary,
    notify_fatal_error,
    notify_pipeline_result,
    notify_quota_warning,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def config_with_slack():
    return Config(slack_webhook_url="https://hooks.slack.com/services/T00/B00/xxx")


@pytest.fixture
def config_no_slack():
    return Config(slack_webhook_url="")


@pytest.fixture
def news_stats():
    return {"articles": 12, "mentions": 12, "errors": 0}


@pytest.fixture
def yt_stats():
    return {"videos": 8, "mentions": 8, "errors": 0, "quota_used": 303}


@pytest.fixture
def failed_news_stats():
    return {"articles": 0, "mentions": 0, "errors": 2}


@pytest.fixture
def failed_yt_stats():
    return {"videos": 0, "mentions": 0, "errors": 1, "quota_used": 100}


# ─── DriftEvent model ────────────────────────────────────────────────────────


class TestDriftEvent:
    def test_first_observation(self):
        event = DriftEvent(
            api_source="newsapi",
            endpoint="/v2/everything",
            previous_hash=None,
            new_hash="abc123",
        )
        assert event.is_first_observation is True
        assert event.is_actual_change is False

    def test_actual_change(self):
        event = DriftEvent(
            api_source="youtube",
            endpoint="search.list",
            previous_hash="old_hash",
            new_hash="new_hash",
        )
        assert event.is_first_observation is False
        assert event.is_actual_change is True

    def test_key_paths_default_empty(self):
        event = DriftEvent(
            api_source="newsapi", endpoint="test", previous_hash=None, new_hash="abc"
        )
        assert event.key_paths == []


# ─── Core: Webhook not configured ────────────────────────────────────────────


def test_no_webhook_returns_false(config_no_slack, news_stats, yt_stats):
    """All notifications should silently return False when no webhook is set."""
    assert (
        notify_pipeline_result(config_no_slack, "run-1", "success", news_stats, yt_stats, quota=0) is False
    )
    assert notify_drift_summary(config_no_slack, [], run_id="run-1") is False
    assert notify_quota_warning(config_no_slack, 9000) is False
    assert notify_fatal_error(config_no_slack, "newsapi", "auth_failure", "Bad key") is False


# ─── notify_pipeline_result ───────────────────────────────────────────────────


@patch("src.notifications.requests.post")
def test_pipeline_success_sends_message(mock_post, config_with_slack, news_stats, yt_stats):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_pipeline_result(
        config_with_slack, "run-001", "success", news_stats, yt_stats, quota=0, duration_seconds=4.2
    )

    assert result is True
    mock_post.assert_called_once()

    payload = mock_post.call_args[1]["json"]
    blocks_text = str(payload)
    assert "SUCCESS" in blocks_text
    assert "12 articles" in blocks_text
    assert "8 videos" in blocks_text


@patch("src.notifications.requests.post")
def test_pipeline_failure_sends_message(
    mock_post, config_with_slack, failed_news_stats, failed_yt_stats
):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_pipeline_result(
        config_with_slack, "run-002", "failed", failed_news_stats, failed_yt_stats, quota=0
    )

    assert result is True
    payload = mock_post.call_args[1]["json"]
    assert "FAILED" in str(payload)


@patch("src.notifications.requests.post")
def test_pipeline_partial_sends_message(mock_post, config_with_slack, news_stats, failed_yt_stats):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_pipeline_result(
        config_with_slack, "run-003", "partial", news_stats, failed_yt_stats, quota=0
    )

    assert result is True
    assert "PARTIAL" in str(mock_post.call_args[1]["json"])


@patch("src.notifications.requests.post")
def test_pipeline_result_includes_quota(mock_post, config_with_slack, news_stats, yt_stats):
    mock_post.return_value = MagicMock(status_code=200)

    notify_pipeline_result(config_with_slack, "run-004", "success", news_stats, yt_stats, quota=0)

    payload_text = str(mock_post.call_args[1]["json"])
    assert "303" in payload_text


# ─── notify_drift_summary ────────────────────────────────────────────────────


def test_drift_summary_skips_first_observations_only(config_with_slack):
    """A run with ONLY first observations should not send any Slack alert."""
    events = [
        DriftEvent("newsapi", "/v2/everything", previous_hash=None, new_hash="aaa"),
        DriftEvent("youtube", "search.list", previous_hash=None, new_hash="bbb"),
        DriftEvent("youtube", "videos.list", previous_hash=None, new_hash="ccc"),
    ]
    result = notify_drift_summary(config_with_slack, events, run_id="run-010")
    assert result is False  # no Slack message sent


def test_drift_summary_empty_list(config_with_slack):
    """No drift events at all should not send."""
    assert notify_drift_summary(config_with_slack, [], run_id="run-011") is False


@patch("src.notifications.requests.post")
def test_drift_summary_sends_for_actual_changes(mock_post, config_with_slack):
    """Actual structural changes should trigger exactly one notification."""
    mock_post.return_value = MagicMock(status_code=200)

    events = [
        # First observations (should be filtered out)
        DriftEvent("newsapi", "/v2/everything", previous_hash=None, new_hash="aaa"),
        # Actual change (should trigger alert)
        DriftEvent(
            "youtube",
            "search.list",
            previous_hash="old_hash_123456",
            new_hash="new_hash_789012",
            key_paths=["items[].snippet.title", "items[].snippet.channelTitle"],
        ),
    ]

    result = notify_drift_summary(config_with_slack, events, run_id="run-012")

    assert result is True
    mock_post.assert_called_once()  # exactly ONE Slack message

    payload_text = str(mock_post.call_args[1]["json"])
    assert "Drift" in payload_text
    assert "search.list" in payload_text
    assert "old_hash_123" in payload_text
    # First observation should NOT appear in the message
    assert "/v2/everything" not in payload_text


@patch("src.notifications.requests.post")
def test_drift_summary_deduplicates_by_endpoint(mock_post, config_with_slack):
    """Multiple drift events on the same endpoint should be collapsed to one."""
    mock_post.return_value = MagicMock(status_code=200)

    events = [
        # Same endpoint drifted across 3 query terms — should collapse to 1
        DriftEvent("youtube", "search.list", previous_hash="hash_v1", new_hash="hash_v2a"),
        DriftEvent("youtube", "search.list", previous_hash="hash_v2a", new_hash="hash_v2b"),
        DriftEvent("youtube", "search.list", previous_hash="hash_v2b", new_hash="hash_v2c"),
    ]

    result = notify_drift_summary(config_with_slack, events, run_id="run-013")

    assert result is True
    mock_post.assert_called_once()

    payload_text = str(mock_post.call_args[1]["json"])
    assert "1 endpoint(s)" in payload_text
    # Should keep the latest event (hash_v2c)
    assert "hash_v2c" in payload_text


@patch("src.notifications.requests.post")
def test_drift_summary_multiple_endpoints(mock_post, config_with_slack):
    """Changes on different endpoints should each appear in the notification."""
    mock_post.return_value = MagicMock(status_code=200)

    events = [
        DriftEvent("youtube", "search.list", previous_hash="old1", new_hash="new1"),
        DriftEvent("youtube", "videos.list", previous_hash="old2", new_hash="new2"),
    ]

    result = notify_drift_summary(config_with_slack, events, run_id="run-014")

    assert result is True
    payload_text = str(mock_post.call_args[1]["json"])
    assert "search.list" in payload_text
    assert "videos.list" in payload_text
    assert "2 endpoint(s)" in payload_text


@patch("src.notifications.requests.post")
def test_drift_summary_mixed_first_and_actual(mock_post, config_with_slack):
    """Mixed events: only actual changes should appear, count should be correct."""
    mock_post.return_value = MagicMock(status_code=200)

    events = [
        DriftEvent("newsapi", "/v2/everything", previous_hash=None, new_hash="aaa"),
        DriftEvent("youtube", "search.list", previous_hash=None, new_hash="bbb"),
        DriftEvent("youtube", "videos.list", previous_hash="old_vid", new_hash="new_vid"),
    ]

    result = notify_drift_summary(config_with_slack, events, run_id="run-015")

    assert result is True
    payload_text = str(mock_post.call_args[1]["json"])
    assert "1 endpoint(s)" in payload_text
    assert "videos.list" in payload_text


# ─── notify_quota_warning ─────────────────────────────────────────────────────


@patch("src.notifications.requests.post")
def test_quota_warning_fires_above_threshold(mock_post, config_with_slack):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_quota_warning(config_with_slack, quota_used=8500, run_id="run-020")

    assert result is True
    payload_text = str(mock_post.call_args[1]["json"])
    assert "8,500" in payload_text
    assert "Quota" in payload_text


def test_quota_warning_skips_below_threshold(config_with_slack):
    """Should not send a notification if usage is below 80%."""
    result = notify_quota_warning(config_with_slack, quota_used=5000)
    assert result is False


# ─── notify_fatal_error ───────────────────────────────────────────────────────


@patch("src.notifications.requests.post")
def test_fatal_error_auth(mock_post, config_with_slack):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_fatal_error(
        config_with_slack,
        api_source="newsapi",
        error_type="auth_failure",
        error_message="API key revoked",
        query_term="Avenue Z",
        run_id="run-030",
    )

    assert result is True
    payload_text = str(mock_post.call_args[1]["json"])
    assert "Fatal" in payload_text
    assert "auth_failure" in payload_text
    assert "API key revoked" in payload_text
    assert "Avenue Z" in payload_text


@patch("src.notifications.requests.post")
def test_fatal_error_quota(mock_post, config_with_slack):
    mock_post.return_value = MagicMock(status_code=200)

    result = notify_fatal_error(
        config_with_slack,
        api_source="youtube",
        error_type="quota_exceeded",
        error_message="Daily quota exhausted",
    )

    assert result is True
    assert "quota_exceeded" in str(mock_post.call_args[1]["json"])


# ─── Error handling: webhook failures ─────────────────────────────────────────


@patch("src.notifications.requests.post")
def test_webhook_http_error_returns_false(mock_post, config_with_slack, news_stats, yt_stats):
    mock_post.return_value = MagicMock(status_code=500, text="Internal Server Error")

    result = notify_pipeline_result(config_with_slack, "run-x", "success", news_stats, yt_stats, quota=0)
    assert result is False


@patch("src.notifications.requests.post")
def test_webhook_timeout_returns_false(mock_post, config_with_slack, news_stats, yt_stats):
    mock_post.side_effect = __import__("requests").exceptions.Timeout()

    result = notify_pipeline_result(config_with_slack, "run-x", "success", news_stats, yt_stats, quota=0)
    assert result is False


@patch("src.notifications.requests.post")
def test_webhook_connection_error_returns_false(mock_post, config_with_slack, news_stats, yt_stats):
    mock_post.side_effect = __import__("requests").exceptions.ConnectionError()

    result = notify_pipeline_result(config_with_slack, "run-x", "success", news_stats, yt_stats, quota=0)
    assert result is False
