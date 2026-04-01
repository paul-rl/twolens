"""
TwoLens — YouTube Response Models
───────────────────────────────────
Pydantic models for the YouTube Data API v3. Two endpoints are used:

  1. search.list  → returns video IDs, basic snippet data
  2. videos.list  → returns full statistics, tags, contentDetails

The search response and video detail response have very different shapes,
so we model them separately. The client merges them before transform.

YouTube's responses are deeply nested (snippet, statistics, contentDetails
are separate top-level keys per item), making this a strong contrast to
NewsAPI's flat structure — good for demonstrating schema normalization.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

# ─── Search Response Models ───────────────────────────────────────────────────


class SearchThumbnail(BaseModel):
    url: str = ""
    width: int | None = None
    height: int | None = None


class SearchThumbnails(BaseModel):
    model_config = {"extra": "allow"}
    default: SearchThumbnail = Field(default_factory=SearchThumbnail)


class SearchSnippet(BaseModel):
    model_config = {"extra": "allow"}

    published_at: str | None = Field(None, alias="publishedAt")
    channel_id: str | None = Field(None, alias="channelId")
    title: str = ""
    description: str | None = None
    channel_title: str | None = Field(None, alias="channelTitle")
    thumbnails: SearchThumbnails = Field(default_factory=SearchThumbnails)
    live_broadcast_ontent: str | None = Field(None, alias="liveBroadcastContent")

    @field_validator("title", mode="before")
    @classmethod
    def clean_title(cls, v: str | None) -> str:
        if v is None:
            return ""
        return v.strip()

    @field_validator("published_at", mode="before")
    @classmethod
    def validate_timestamp(cls, v: str | None) -> str | None:
        if not v:
            return None
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except (ValueError, TypeError):
            return None


class SearchVideoId(BaseModel):
    kind: str = ""
    video_id: str = Field("", alias="videoId")


class SearchItem(BaseModel):
    model_config = {"extra": "allow"}

    kind: str = ""
    etag: str = ""
    id: SearchVideoId = Field(default_factory=SearchVideoId)
    snippet: SearchSnippet = Field(default_factory=SearchSnippet)

    @property
    def video_id(self) -> str:
        return self.id.video_id

    @property
    def is_valid(self) -> bool:
        return bool(self.video_id) and bool(self.snippet.title)

    @property
    def extra_fields(self) -> set[str]:
        return set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()


class YouTubeSearchResponse(BaseModel):
    model_config = {"extra": "allow"}

    kind: str = ""
    etag: str = ""
    next_page_token: str | None = Field(None, alias="nextPageToken")
    region_code: str | None = Field(None, alias="regionCode")
    page_info: dict[str, Any] = Field(default_factory=dict, alias="pageInfo")
    items: list[SearchItem] = Field(default_factory=list)

    @property
    def valid_items(self) -> list[SearchItem]:
        return [item for item in self.items if item.is_valid]

    @property
    def video_ids(self) -> list[str]:
        return [item.video_id for item in self.valid_items]

    @property
    def extra_fields(self) -> set[str]:

        return set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()


# ─── Video Detail Response Models ─────────────────────────────────────────────


class VideoStatistics(BaseModel):
    """All stats come as strings from the API — we coerce to int."""

    model_config = {"extra": "allow"}

    view_count: int = Field(0, alias="viewCount")
    like_count: int = Field(0, alias="likeCount")
    comment_count: int = Field(0, alias="commentCount")

    @field_validator("view_count", "like_count", "comment_count", mode="before")
    @classmethod
    def coerce_to_int(cls, v: Any) -> int:
        if not v:
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @property
    def views(self) -> int:
        try:
            return int(self.view_count)
        except (ValueError, TypeError):
            return 0

    @property
    def likes(self) -> int:
        try:
            return int(self.like_count)
        except (ValueError, TypeError):
            return 0

    @property
    def comments(self) -> int:
        try:
            return int(self.comment_count)
        except (ValueError, TypeError):
            return 0


class VideoContentDetails(BaseModel):
    model_config = {"extra": "allow"}

    duration: str = ""  # ISO 8601 duration, e.g. 'PT4M13S'


class VideoSnippet(BaseModel):
    model_config = {"extra": "allow"}

    published_at: str | None = Field(None, alias="publishedAt")
    channel_id: str | None = Field(None, alias="channelId")
    title: str = ""
    description: str | None = None
    channel_title: str | None = Field(None, alias="channelTitle")
    tags: list[str] = Field(default_factory=list)
    category_id: str | None = Field(None, alias="categoryId")
    thumbnails: SearchThumbnails = Field(default_factory=SearchThumbnails)

    @field_validator("title", mode="before")
    @classmethod
    def clean_title(cls, v: str | None) -> str:
        if v is None:
            return ""
        return v.strip()


class VideoItem(BaseModel):
    model_config = {"extra": "allow"}

    kind: str = ""
    etag: str = ""
    id: str = ""
    snippet: VideoSnippet = Field(default_factory=VideoSnippet)
    statistics: VideoStatistics = Field(default_factory=VideoStatistics)
    content_details: VideoContentDetails = Field(
        default_factory=VideoContentDetails, alias="contentDetails"
    )

    @property
    def is_valid(self) -> bool:
        return bool(self.id) and bool(self.snippet.title)

    @property
    def extra_fields(self) -> set[str]:
        return set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()


class YouTubeVideoResponse(BaseModel):
    model_config = {"extra": "allow"}

    kind: str = ""
    etag: str = ""
    page_info: dict[str, Any] = Field(default_factory=dict, alias="pageInfo")
    items: list[VideoItem] = Field(default_factory=list)

    @property
    def extra_fields(self) -> set[str]:
        return set(self.__pydantic_extra__.keys()) if self.__pydantic_extra__ else set()
