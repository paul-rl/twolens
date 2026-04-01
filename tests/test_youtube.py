"""Tests for the YouTube client module."""

from unittest.mock import MagicMock, patch

import pytest

from src.clients.shared.fetch_model import FetchResult
from src.clients.youtube.client import (
    YouTubeSearchResponse,
    YouTubeVideoResponse,
    _generate_video_id,
    _parse_timestamp,
    build_error_row,
    estimate_quota_used,
    fetch_search,
    fetch_video_details,
    transform_to_brand_mentions,
    transform_to_youtube_videos,
    validate_search_response,
    validate_video_response,
)
from src.config import Config

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def config():
    return Config(
        youtube_api_key="test_yt_key",
        youtube_base_url="https://www.googleapis.com/youtube/v3",
        max_results_per_query=10,
        request_timeout=5,
    )


@pytest.fixture
def sample_search_response():
    return {
        "kind": "youtube#searchListResponse",
        "pageInfo": {"totalResults": 2, "resultsPerPage": 10},
        "items": [
            {
                "kind": "youtube#searchResult",
                "id": {"kind": "youtube#video", "videoId": "abc123"},
                "snippet": {
                    "publishedAt": "2026-03-30T14:00:00Z",
                    "channelId": "UC_chan1",
                    "title": "Avenue Z AI Platform Review",
                    "description": "My thoughts on the new platform...",
                    "channelTitle": "TechReviewer",
                    "thumbnails": {"default": {"url": "https://img.youtube.com/abc.jpg"}},
                },
            },
            {
                "kind": "youtube#searchResult",
                "id": {"kind": "youtube#video", "videoId": "def456"},
                "snippet": {
                    "publishedAt": "2026-03-29T10:00:00Z",
                    "channelId": "UC_chan2",
                    "title": "Avenue Z Marketing Deep Dive",
                    "description": "Breaking down their strategy...",
                    "channelTitle": "MarketingPro",
                    "thumbnails": {"default": {"url": "https://img.youtube.com/def.jpg"}},
                },
            },
        ],
    }


@pytest.fixture
def sample_video_response():
    return {
        "kind": "youtube#videoListResponse",
        "pageInfo": {"totalResults": 2, "resultsPerPage": 2},
        "items": [
            {
                "kind": "youtube#video",
                "id": "abc123",
                "snippet": {
                    "publishedAt": "2026-03-30T14:00:00Z",
                    "channelId": "UC_chan1",
                    "title": "Avenue Z AI Platform Review",
                    "description": "Full description here...",
                    "channelTitle": "TechReviewer",
                    "tags": ["avenue z", "ai", "marketing"],
                    "categoryId": "22",
                    "thumbnails": {"default": {"url": "https://img.youtube.com/abc.jpg"}},
                },
                "statistics": {
                    "viewCount": "15234",
                    "likeCount": "892",
                    "commentCount": "47",
                },
                "contentDetails": {"duration": "PT12M35S"},
            },
            {
                "kind": "youtube#video",
                "id": "def456",
                "snippet": {
                    "publishedAt": "2026-03-29T10:00:00Z",
                    "channelId": "UC_chan2",
                    "title": "Avenue Z Marketing Deep Dive",
                    "description": "Strategy breakdown...",
                    "channelTitle": "MarketingPro",
                    "tags": ["marketing", "agency"],
                    "categoryId": "27",
                    "thumbnails": {"default": {"url": "https://img.youtube.com/def.jpg"}},
                },
                "statistics": {
                    "viewCount": "5678",
                    "likeCount": "234",
                    "commentCount": "12",
                },
                "contentDetails": {"duration": "PT8M22S"},
            },
        ],
    }


@pytest.fixture
def search_result(sample_search_response):
    return FetchResult(raw_response=sample_search_response, http_status=200)


@pytest.fixture
def video_result(sample_video_response):
    return FetchResult(raw_response=sample_video_response, http_status=200)


# ─── Unit: Helpers ────────────────────────────────────────────────────────────


def test_generate_video_id():
    assert _generate_video_id("abc123") == "yt_abc123"


def test_parse_timestamp_valid():
    result = _parse_timestamp("2026-03-30T14:00:00Z")
    assert result is not None
    assert "2026-03-30" in result


def test_parse_timestamp_none():
    assert _parse_timestamp(None) is None


def test_parse_timestamp_invalid():
    assert _parse_timestamp("garbage") is None


# ─── Unit: Pydantic Models ────────────────────────────────────────────────────


class TestSearchResponse:
    def test_parse_valid(self, sample_search_response):
        parsed = YouTubeSearchResponse.model_validate(sample_search_response)
        assert len(parsed.items) == 2
        assert len(parsed.valid_items) == 2
        assert parsed.video_ids == ["abc123", "def456"]

    def test_filters_invalid_items(self):
        raw = {
            "items": [
                {"id": {"videoId": "good"}, "snippet": {"title": "Valid"}},
                {"id": {"videoId": ""}, "snippet": {"title": "No ID"}},
                {"id": {"videoId": "nope"}, "snippet": {"title": ""}},
            ],
        }
        parsed = YouTubeSearchResponse.model_validate(raw)
        assert len(parsed.valid_items) == 1
        assert parsed.video_ids == ["good"]

    def test_extra_fields_captured(self):
        raw = {"items": [], "newField": "surprise"}
        parsed = YouTubeSearchResponse.model_validate(raw)
        assert "newField" in parsed.extra_fields


class TestVideoResponse:
    def test_parse_valid(self, sample_video_response):
        parsed = YouTubeVideoResponse.model_validate(sample_video_response)
        assert len(parsed.items) == 2
        assert parsed.items[0].statistics.views == 15234
        assert parsed.items[0].statistics.likes == 892
        assert parsed.items[0].statistics.comments == 47

    def test_statistics_handle_non_numeric(self):
        raw = {
            "items": [
                {
                    "id": "test",
                    "snippet": {"title": "Test"},
                    "statistics": {
                        "viewCount": "not_a_number",
                        "likeCount": "",
                        "commentCount": None,
                    },
                    "contentDetails": {},
                }
            ]
        }
        parsed = YouTubeVideoResponse.model_validate(raw)
        assert parsed.items[0].statistics.views == 0
        assert parsed.items[0].statistics.likes == 0

    def test_missing_statistics_uses_defaults(self):
        raw = {"items": [{"id": "test", "snippet": {"title": "Test"}}]}
        parsed = YouTubeVideoResponse.model_validate(raw)
        assert parsed.items[0].statistics.views == 0

    def test_extra_fields_on_video(self):
        raw = {
            "items": [
                {
                    "id": "test",
                    "snippet": {"title": "T"},
                    "statistics": {},
                    "contentDetails": {},
                    "brandNewField": True,
                }
            ]
        }
        parsed = YouTubeVideoResponse.model_validate(raw)
        assert "brandNewField" in parsed.items[0].extra_fields


# ─── Unit: fetch_search ───────────────────────────────────────────────────────


@patch("src.clients.youtube.client.requests.get")
def test_fetch_search_success(mock_get, config, sample_search_response):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_search_response
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_search(config, "Avenue Z")

    assert isinstance(result, FetchResult)
    assert not result.is_error
    assert result.http_status == 200


@patch("src.clients.youtube.client.requests.get")
def test_fetch_search_quota_exceeded(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_resp.json.return_value = {"error": {"errors": [{"reason": "quotaExceeded"}]}}
    mock_get.return_value = mock_resp

    result = fetch_search(config, "test")

    assert result.is_error
    assert result.error_type == "quota_exceeded"


@patch("src.clients.youtube.client.requests.get")
def test_fetch_search_missing_items(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"kind": "youtube#searchListResponse"}
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_search(config, "test")

    assert result.is_error
    assert result.error_type == "schema_drift"


@patch("src.clients.youtube.client.requests.get")
def test_fetch_search_timeout(mock_get, config):
    mock_get.side_effect = __import__("requests").exceptions.Timeout()

    result = fetch_search(config, "test")

    assert result.is_error
    assert result.error_type == "timeout"


@patch("src.clients.youtube.client.requests.get")
def test_fetch_search_bad_request(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 400
    mock_get.return_value = mock_resp

    result = fetch_search(config, "test")

    assert result.is_error
    assert result.error_type == "auth_failure"
    assert result.http_status == 400


# ─── Unit: fetch_video_details ────────────────────────────────────────────────


@patch("src.clients.youtube.client.requests.get")
def test_fetch_video_details_success(mock_get, config, sample_video_response):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_video_response
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_video_details(config, ["abc123", "def456"])

    assert not result.is_error
    assert len(result.raw_response["items"]) == 2


def test_fetch_video_details_empty_ids(config):
    """Empty video ID list should return empty items without making API call."""
    result = fetch_video_details(config, [])
    assert not result.is_error
    assert result.raw_response == {"items": []}


# ─── Unit: validate ───────────────────────────────────────────────────────────


def test_validate_search_success(search_result):
    parsed, warnings = validate_search_response(search_result)
    assert parsed is not None
    assert len(parsed.valid_items) == 2


def test_validate_search_skips_errors():
    error = FetchResult(raw_response={}, is_error=True, error_type="timeout")
    parsed, warnings = validate_search_response(error)
    assert parsed is None


def test_validate_video_success(video_result):
    parsed, warnings = validate_video_response(video_result)
    assert parsed is not None
    assert len(parsed.items) == 2


# ─── Unit: transform_to_youtube_videos ────────────────────────────────────────


def test_transform_videos(sample_video_response):
    parsed = YouTubeVideoResponse.model_validate(sample_video_response)
    rows = transform_to_youtube_videos(parsed, "Avenue Z", "run-001")

    assert len(rows) == 2
    assert rows[0]["video_id"] == "abc123"
    assert rows[0]["view_count"] == 15234
    assert rows[0]["like_count"] == 892
    assert rows[0]["channel_title"] == "TechReviewer"
    assert rows[0]["url"] == "https://www.youtube.com/watch?v=abc123"
    assert rows[0]["query_term"] == "Avenue Z"


def test_transform_videos_missing_stats():
    raw = {
        "items": [
            {
                "id": "vid1",
                "snippet": {"title": "No Stats Video", "channelTitle": "Chan"},
                "contentDetails": {},
            }
        ]
    }
    parsed = YouTubeVideoResponse.model_validate(raw)
    rows = transform_to_youtube_videos(parsed, "test", "run-002")

    assert len(rows) == 1
    assert rows[0]["view_count"] == 0
    assert rows[0]["like_count"] == 0


# ─── Unit: transform_to_brand_mentions ────────────────────────────────────────


def test_brand_mentions_from_youtube(sample_video_response):
    parsed = YouTubeVideoResponse.model_validate(sample_video_response)
    videos = transform_to_youtube_videos(parsed, "Avenue Z", "run-003")
    mentions = transform_to_brand_mentions(videos)

    assert len(mentions) == 2
    assert mentions[0]["source_platform"] == "youtube"
    assert mentions[0]["mention_type"] == "youtube_video"
    assert mentions[0]["mention_id"] == "yt_abc123"
    assert mentions[0]["engagement_score"] == 15234
    assert mentions[0]["like_count"] == 892
    assert mentions[0]["source_detail"] == "TechReviewer"


# ─── Unit: build_error_row ────────────────────────────────────────────────────


def test_error_row_quota_exceeded():
    result = FetchResult(
        raw_response={},
        http_status=403,
        is_error=True,
        error_type="quota_exceeded",
        error_message="Quota gone",
    )
    row = build_error_row(result, "test", "run-004")

    assert row is not None
    assert row["api_source"] == "youtube"
    assert row["error_type"] == "quota_exceeded"
    assert row["severity"] == "critical"


def test_error_row_returns_none_for_success():
    result = FetchResult(raw_response={"items": []}, http_status=200)
    assert build_error_row(result, "test", "run-005") is None


# ─── Unit: estimate_quota_used ────────────────────────────────────────────────


def test_quota_estimation():
    assert estimate_quota_used(1, 1) == 101
    assert estimate_quota_used(3, 3) == 303
    assert estimate_quota_used(0, 0) == 0
