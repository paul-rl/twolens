from dataclasses import dataclass
from typing import Any


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
    error_type: str | None = None  # 'timeout' | 'rate_limit' | 'auth_failure' | etc.
    error_message: str | None = None
    response_snippet: str | None = None  # first 1000 chars on unexpected responses
