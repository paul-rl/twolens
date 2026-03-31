"""Tests for the NewsAPI client module (with Pydantic validation + FetchResult)."""

from unittest.mock import MagicMock, patch

import pytest

from src.clients.newsapi import (
    _generate_article_id,
    build_error_row,
    build_raw_response_row,
    fetch_articles,
    transform_to_brand_mentions,
    transform_to_news_articles,
    validate_response,
)
from src.clients.newsapi_models import FetchResult, NewsApiResponse, NewsArticle, NewsSource
from src.config import Config

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def config():
    return Config(
        newsapi_key="test_key_123",
        newsapi_base_url="https://newsapi.org/v2",
        max_results_per_query=10,
        request_timeout=5,
    )


@pytest.fixture
def sample_raw_response():
    """A realistic raw NewsAPI response dict (pre-validation)."""
    return {
        "status": "ok",
        "totalResults": 2,
        "articles": [
            {
                "source": {"id": "bbc-news", "name": "BBC News"},
                "author": "Jane Doe",
                "title": "Avenue Z Launches New AI Platform",
                "description": "Marketing agency Avenue Z unveils AI tools.",
                "url": "https://bbc.com/article/123",
                "urlToImage": "https://bbc.com/img/123.jpg",
                "publishedAt": "2026-03-30T14:00:00Z",
                "content": "Avenue Z, a digital marketing agency, today announced...",
            },
            {
                "source": {"id": None, "name": "TechCrunch"},
                "author": None,
                "title": "Avenue Z Raises Series B",
                "description": "The agency secured $50M in funding.",
                "url": "https://techcrunch.com/article/456",
                "urlToImage": None,
                "publishedAt": "2026-03-29T10:30:00Z",
                "content": "Avenue Z announced a $50M Series B round...",
            },
        ],
    }


@pytest.fixture
def success_result(sample_raw_response):
    """A successful FetchResult wrapping a sample response."""
    return FetchResult(raw_response=sample_raw_response, http_status=200)


@pytest.fixture
def error_result():
    """A failed FetchResult."""
    return FetchResult(
        raw_response={},
        http_status=429,
        is_error=True,
        error_type="rate_limit",
        error_message="Rate limit exceeded",
    )


# ─── Unit: Helpers ────────────────────────────────────────────────────────────


def test_generate_article_id_is_deterministic():
    id1 = _generate_article_id("BBC News", "2026-03-30T14:00:00Z", "Test Title")
    id2 = _generate_article_id("BBC News", "2026-03-30T14:00:00Z", "Test Title")
    assert id1 == id2


def test_generate_article_id_differs_on_different_input():
    id1 = _generate_article_id("BBC News", "2026-03-30T14:00:00Z", "Title A")
    id2 = _generate_article_id("BBC News", "2026-03-30T14:00:00Z", "Title B")
    assert id1 != id2


# ─── Unit: FetchResult ────────────────────────────────────────────────────────


class TestFetchResult:
    def test_success_result(self, success_result):
        assert not success_result.is_error
        assert success_result.http_status == 200
        assert "articles" in success_result.raw_response

    def test_error_result(self, error_result):
        assert error_result.is_error
        assert error_result.http_status == 429
        assert error_result.error_type == "rate_limit"
        assert error_result.raw_response == {}

    def test_default_values(self):
        result = FetchResult(raw_response={"test": True})
        assert result.http_status is None
        assert not result.is_error
        assert result.error_type is None


# ─── Unit: Pydantic Models ────────────────────────────────────────────────────


class TestNewsArticleModel:
    def test_valid_article(self):
        article = NewsArticle(
            source=NewsSource(id="bbc", name="BBC News"),
            title="Test Article",
            published_at="2026-03-30T14:00:00Z",
        )
        assert article.is_valid
        assert article.source.name == "BBC News"

    def test_empty_title_is_invalid(self):
        article = NewsArticle(title="")
        assert not article.is_valid

    def test_removed_title_is_invalid(self):
        article = NewsArticle(title="[Removed]")
        assert not article.is_valid

    def test_none_title_becomes_empty_string(self):
        article = NewsArticle(title=None)
        assert article.title == ""
        assert not article.is_valid

    def test_bad_timestamp_becomes_none(self):
        article = NewsArticle(title="Test", published_at="not-a-date")
        assert article.published_at is None

    def test_missing_fields_use_defaults(self):
        article = NewsArticle(title="Minimal Article")
        assert article.is_valid
        assert article.author is None
        assert article.source.name == "Unknown"

    def test_extra_fields_are_captured(self):
        article = NewsArticle(
            title="Test",
            new_field_from_api="surprise!",
            another_new_one=42,
        )
        assert article.is_valid
        assert "new_field_from_api" in article.extra_fields
        assert "another_new_one" in article.extra_fields


class TestNewsApiResponseModel:
    def test_valid_response(self, sample_raw_response):
        parsed = NewsApiResponse.model_validate(sample_raw_response)
        assert parsed.status == "ok"
        assert len(parsed.articles) == 2
        assert len(parsed.valid_articles) == 2

    def test_filters_invalid_articles(self):
        raw = {
            "status": "ok",
            "articles": [
                {"title": "Good Article", "source": {"name": "BBC"}},
                {"title": "[Removed]", "source": {"name": "X"}},
                {"title": "", "source": {"name": "Y"}},
            ],
        }
        parsed = NewsApiResponse.model_validate(raw)
        assert len(parsed.articles) == 3
        assert len(parsed.valid_articles) == 1

    def test_extra_top_level_fields(self):
        raw = {"status": "ok", "articles": [], "newField": "hello"}
        parsed = NewsApiResponse.model_validate(raw)
        assert "newField" in parsed.extra_fields

    def test_all_article_fields_includes_extras(self):
        raw = {"articles": [{"title": "Test", "brand_new_field": "data"}]}
        parsed = NewsApiResponse.model_validate(raw)
        assert "brand_new_field" in parsed.all_article_fields
        assert "title" in parsed.all_article_fields


# ─── Unit: fetch_articles ─────────────────────────────────────────────────────


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_success(mock_get, config, sample_raw_response):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_raw_response
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_articles(config, "Avenue Z")

    assert isinstance(result, FetchResult)
    assert not result.is_error
    assert result.http_status == 200
    assert result.raw_response["status"] == "ok"


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_auth_failure(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_get.return_value = mock_resp

    result = fetch_articles(config, "Avenue Z")

    assert result.is_error
    assert result.http_status == 401
    assert result.error_type == "auth_failure"


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_rate_limit(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    mock_get.return_value = mock_resp

    result = fetch_articles(config, "Avenue Z")

    assert result.is_error
    assert result.http_status == 429
    assert result.error_type == "rate_limit"


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_schema_drift(mock_get, config):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"status": "ok", "data": []}
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    result = fetch_articles(config, "Avenue Z")

    assert result.is_error
    assert result.http_status == 200
    assert result.error_type == "schema_drift"
    assert result.raw_response == {"status": "ok", "data": []}


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_timeout(mock_get, config):
    mock_get.side_effect = __import__("requests").exceptions.Timeout()

    result = fetch_articles(config, "Avenue Z")

    assert result.is_error
    assert result.http_status is None
    assert result.error_type == "timeout"


@patch("src.clients.newsapi.requests.get")
def test_fetch_articles_connection_error(mock_get, config):
    mock_get.side_effect = __import__("requests").exceptions.ConnectionError()

    result = fetch_articles(config, "Avenue Z")

    assert result.is_error
    assert result.error_type == "http_error"


# ─── Unit: validate_response ──────────────────────────────────────────────────


def test_validate_response_success(success_result):
    parsed, warnings = validate_response(success_result)
    assert parsed is not None
    assert len(parsed.valid_articles) == 2
    assert len(warnings) == 0


def test_validate_response_with_extra_fields():
    result = FetchResult(
        raw_response={
            "status": "ok",
            "articles": [{"title": "Test", "unexpected_field": "value"}],
            "new_top_level": True,
        },
        http_status=200,
    )
    parsed, warnings = validate_response(result)
    assert parsed is not None
    assert any("New top-level fields" in w for w in warnings)
    assert any("New fields detected" in w for w in warnings)


def test_validate_response_skips_errors(error_result):
    parsed, warnings = validate_response(error_result)
    assert parsed is None
    assert warnings == []


# ─── Unit: transform_to_news_articles ─────────────────────────────────────────


def test_transform_success(sample_raw_response):
    parsed = NewsApiResponse.model_validate(sample_raw_response)
    rows = transform_to_news_articles(parsed, "Avenue Z", "run-001")

    assert len(rows) == 2
    assert rows[0]["title"] == "Avenue Z Launches New AI Platform"
    assert rows[0]["source_name"] == "BBC News"
    assert rows[0]["pipeline_run_id"] == "run-001"


def test_transform_handles_missing_fields():
    raw = {"articles": [{"source": {}, "title": "Partial", "publishedAt": "2026-03-30T14:00:00Z"}]}
    parsed = NewsApiResponse.model_validate(raw)
    rows = transform_to_news_articles(parsed, "test", "run-002")

    assert len(rows) == 1
    assert rows[0]["source_name"] == "Unknown"
    assert rows[0]["author"] is None


def test_transform_skips_removed_articles():
    raw = {
        "articles": [
            {"source": {"name": "X"}, "title": "[Removed]", "publishedAt": "2026-03-30T14:00:00Z"},
            {
                "source": {"name": "Y"},
                "title": "Real Article",
                "publishedAt": "2026-03-30T14:00:00Z",
            },
        ],
    }
    parsed = NewsApiResponse.model_validate(raw)
    rows = transform_to_news_articles(parsed, "test", "run-003")

    assert len(rows) == 1
    assert rows[0]["title"] == "Real Article"


# ─── Unit: transform_to_brand_mentions ────────────────────────────────────────


def test_brand_mentions_from_news_articles(sample_raw_response):
    parsed = NewsApiResponse.model_validate(sample_raw_response)
    articles = transform_to_news_articles(parsed, "Avenue Z", "run-005")
    mentions = transform_to_brand_mentions(articles)

    assert len(mentions) == 2
    assert mentions[0]["source_platform"] == "newsapi"
    assert mentions[0]["mention_type"] == "news_article"
    assert mentions[0]["mention_id"].startswith("news_")
    assert mentions[0]["engagement_score"] is None
    assert mentions[0]["source_detail"] == "BBC News"


# ─── Unit: build_raw_response_row ─────────────────────────────────────────────


def test_build_raw_response_row(success_result):
    row = build_raw_response_row(success_result, "Avenue Z", "abc123hash", "run-009")

    assert row["api_source"] == "newsapi"
    assert row["http_status"] == 200
    assert row["response_hash"] == "abc123hash"
    assert row["pipeline_run_id"] == "run-009"
    assert '"status": "ok"' in row["response_body"]


def test_build_raw_response_row_on_error(error_result):
    row = build_raw_response_row(error_result, "test", "hash", "run-010")

    assert row["http_status"] == 429
    assert row["response_body"] == "{}"


# ─── Unit: build_error_row ────────────────────────────────────────────────────


def test_build_error_row_from_error(error_result):
    row = build_error_row(error_result, "Avenue Z", "run-006")

    assert row is not None
    assert row["api_source"] == "newsapi"
    assert row["error_type"] == "rate_limit"
    assert row["http_status"] == 429
    assert row["severity"] == "warning"


def test_build_error_row_auth_failure_is_critical():
    result = FetchResult(
        raw_response={},
        http_status=401,
        is_error=True,
        error_type="auth_failure",
        error_message="Bad key",
    )
    row = build_error_row(result, "test", "run-007")
    assert row["severity"] == "critical"


def test_build_error_row_returns_none_for_success(success_result):
    row = build_error_row(success_result, "test", "run-008")
    assert row is None
