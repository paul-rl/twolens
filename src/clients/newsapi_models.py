"""
TwoLens NewsAPI Response Models
──────────────────────────────────
Pydantic models that define the expected API contract. These serve
three purposes:

  1. VALIDATION: catch malformed data per-article, not per-batch
  2. DOCUMENTATION: the models ARE the expected schema, in code
  3. DRIFT DETECTION: unexpected fields are captured via model_config,
     missing required fields trigger warnings (not crashes)

Design principle: permissive defaults everywhere. A missing 'author'
shouldn't kill a pipeline run. But we still want to KNOW it's missing,
so validation errors are logged, not raised.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, HttpUrl

log = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """
    Structured return type for fetch_articles().

    Carries the raw response, HTTP metadata, and error info in one object
    so downstream functions (raw storage, drift detection, transform) each
    get what they need without magic dict keys.
    """

    raw_response: dict[str, Any]
    http_status: int | None = None
    is_error: bool = False
    error_type: str | None = None      # 'timeout' | 'rate_limit' | 'auth_failure' | etc.
    error_message: str | None = None
    response_snippet: str | None = None  # first 1000 chars on unexpected responses


class NewsSource(BaseModel):
    """Nested source object within an article."""

    id: str | None = None
    name: str = "Unknown"


class NewsArticle(BaseModel):
    """
    Single article from NewsAPI's /v2/everything response.

    Every field has a default so that partial articles don't crash
    the pipeline. The model validates types and normalizes data so
    the transform layer can trust what comes out of here.
    """

    # model_config allows extra fields without failing validation.
    # If NewsAPI adds a new field tomorrow, we capture it instead of
    # rejecting the article. Drift detection can compare field sets
    # across runs to spot changes.
    model_config = {"extra": "allow"}

    source: NewsSource = Field(default_factory=NewsSource)
    author: str | None = None
    title: str = ""
    description: str | None = None
    url: HttpUrl | str | None = None
    urlToImage: HttpUrl | str | None = None
    publishedAt: datetime | None = None
    content: str | None = None

    @field_validator("title", mode="before")
    @classmethod
    def clean_title(cls, v: str | None) -> str:
        """Normalize empty/null titles to empty string."""
        if v is None:
            return ""
        return v.strip()

    @field_validator("publishedAt", mode="before")
    @classmethod
    def validate_timestamp(cls, v: str | None) -> datetime | None:
        """Verify timestamp is parseable ISO 8601. Pass through if valid, None if not."""
        if not v:
            return None
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            log.warning(f"NewsAPI: Malformed timestamp '{v}'. Error: {e}")
            return None

    @field_validator("url", "urlToImage", mode="before")
    @classmethod
    def validate_urls(cls, v: str | None) -> str | None:
        """Ensure URLs are somewhat valid, log and nullify if they are complete garbage."""
        if not v:
            return None
        if not v.startswith(("http://", "https://")):
            log.warning(f"NewsAPI: Invalid URL format '{v}'")
            return None
        return v

    @property
    def is_valid(self) -> bool:
        """An article needs at least a non-empty, non-removed title to be usable."""
        return bool(self.title) and self.title != "[Removed]"

    @property
    def extra_fields(self) -> set[str]:
        """Return field names present in the API response but not in our model."""
        # Pydantic stores extra fields in __pydantic_extra__
        extra = set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()
        return extra


class NewsApiResponse(BaseModel):
    """
    Top-level NewsAPI response model.

    Required: 'articles' must be present (otherwise it's a schema drift event).
    Optional: 'status' and 'totalResults' have safe defaults.
    """

    model_config = {"extra": "allow"}

    status: str = "unknown"
    totalResults: int = 0
    articles: list[NewsArticle] = Field(default_factory=list)

    @property
    def valid_articles(self) -> list[NewsArticle]:
        """Articles that pass the is_valid check (have real titles)."""
        return [a for a in self.articles if a.is_valid]

    @property
    def extra_fields(self) -> set[str]:
        """Top-level fields present in API response but not in our model."""
        extra = set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()
        return extra

    @property
    def all_article_fields(self) -> set[str]:
        """Union of all field names across all articles (including extras)."""
        fields: set[str] = set(NewsArticle.model_fields.keys())
        for article in self.articles:
            fields.update(article.extra_fields)
        return fields
