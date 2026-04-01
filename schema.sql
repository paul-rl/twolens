-- =============================================================================
-- TwoLens: Brand Intelligence Pipeline
-- =============================================================================
--
-- DESIGN PHILOSOPHY
-- -----------------
-- "Two lenses" on brand perception:
--   Lens 1 (NewsAPI)  → What the MEDIA says about a brand
--   Lens 2 (YouTube)  → What CREATORS & CONSUMERS say about a brand
--
-- This schema follows a raw → structured → unified pipeline model.
-- Every record is traceable from the unified brand_mentions table back to
-- the original API response. Raw payloads are preserved for replay and
-- debugging. Structured tables normalize each source on its own terms.
-- The unified brand_mentions table merges both sources into a single
-- queryable surface for brand intelligence. The api_contracts table enables
-- schema drift detection — turning API resilience from defensive coding
-- into an observable system.
--
-- BigQuery notes:
--   • BigQuery does not enforce PRIMARY KEY / UNIQUE constraints at insert
--     time — they are declared here as documentation and for query optimizer
--     hints only.
--   • All timestamps are UTC.
--   • Partitioning and clustering are specified where beneficial even on free
--     tier — they cost nothing and improve query performance.
-- =============================================================================


-- =============================================================================
-- 1. RAW LAYER — untransformed API responses for replay & debugging
-- =============================================================================

CREATE TABLE IF NOT EXISTS `twolens.raw_api_responses` (
  response_id       STRING      NOT NULL,   -- UUID generated at capture time
  api_source        STRING      NOT NULL,   -- 'newsapi' | 'youtube'
  endpoint          STRING      NOT NULL,   -- e.g. '/v2/everything', 'youtube/v3/search'
  request_params    JSON,                   -- query params sent (excluding secrets)
  response_body     STRING,                 -- full JSON response body stored as STRING
                                            -- (BigQuery JSON type has size limits;
                                            --  STRING is safer for large payloads)
  response_hash     STRING,                 -- SHA-256 of response structure keys
                                            -- (used for schema drift detection)
  http_status       INT64,                  -- HTTP status code returned
  captured_at       TIMESTAMP   NOT NULL,   -- when we received this response
  pipeline_run_id   STRING      NOT NULL    -- links to pipeline_runs table
)
PARTITION BY DATE(captured_at)
CLUSTER BY api_source;


-- =============================================================================
-- 2. STRUCTURED LAYER — source-specific normalized tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2a. NewsAPI articles  (Lens 1: Media Coverage)
-- -----------------------------------------------------------------------------
-- NewsAPI returns a flat, predictable structure: { articles: [...] }
-- Each article has a consistent shape, making this the "stable" source.
-- Free tier: 100 req/day, articles delayed 24h, content truncated to ~200 chars.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `twolens.news_articles` (
  article_id        STRING      NOT NULL,   -- SHA-256(source_name + published_at + title)
  source_name       STRING,                 -- e.g. 'BBC News', 'TechCrunch'
  source_id         STRING,                 -- NewsAPI source identifier
  author            STRING,
  title             STRING      NOT NULL,
  description       STRING,                 -- article snippet / lead
  content           STRING,                 -- truncated body (NewsAPI free tier limit)
  url               STRING,                 -- link to original article
  image_url         STRING,
  published_at      TIMESTAMP,              -- when the article was published
  query_term        STRING,                 -- the brand/keyword we searched for
  captured_at       TIMESTAMP   NOT NULL,   -- when our pipeline pulled this
  pipeline_run_id   STRING      NOT NULL
)
PARTITION BY DATE(captured_at)
CLUSTER BY query_term, source_name;


-- -----------------------------------------------------------------------------
-- 2b. YouTube videos  (Lens 2: Creator & Consumer Voice)
-- -----------------------------------------------------------------------------
-- YouTube's API requires two calls to get full data:
--   1. search.list  → returns videoId, title, channelTitle, publishedAt
--   2. videos.list  → returns statistics, tags, full description, category
-- This table stores the merged result. The response structure is deeply
-- nested (snippet, statistics, contentDetails are separate top-level keys).
--
-- Free tier: 10,000 units/day. search.list = 100 units, videos.list = 1 unit.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `twolens.youtube_videos` (
  video_id          STRING      NOT NULL,   -- YouTube's video ID (e.g. 'dQw4w9WgXcQ')
  channel_id        STRING,                 -- YouTube channel ID
  channel_title     STRING,                 -- channel display name (creator attribution)
  title             STRING      NOT NULL,   -- video title
  description       STRING,                 -- full video description (often contains links,
                                            -- timestamps, and brand mentions not in the title)
  published_at      TIMESTAMP,              -- when the video was published on YouTube
  tags              STRING,                 -- JSON array of video tags set by creator
                                            -- (stored as STRING for BigQuery compatibility;
                                            --  parse with JSON_EXTRACT_ARRAY in queries)
  category_id       STRING,                 -- YouTube category ID (e.g. '22' = People & Blogs)
  view_count        INT64       DEFAULT NULL,  -- total views at time of capture
  like_count        INT64       DEFAULT NULL,  -- total likes at time of capture
  comment_count     INT64       DEFAULT NULL,  -- total comments at time of capture
  duration          STRING,                 -- ISO 8601 duration (e.g. 'PT4M13S')
  thumbnail_url     STRING,                 -- default thumbnail URL
  url               STRING,                 -- https://www.youtube.com/watch?v={video_id}
  query_term        STRING,                 -- the brand/keyword we searched for
  captured_at       TIMESTAMP   NOT NULL,   -- when our pipeline pulled this
  pipeline_run_id   STRING      NOT NULL
)
PARTITION BY DATE(captured_at)
CLUSTER BY query_term, channel_title;


-- =============================================================================
-- 3. UNIFIED LAYER: single queryable surface for brand intelligence
-- =============================================================================
--
-- This is the table an analyst or downstream tool actually queries.
-- It normalizes both sources into a common shape.
--
-- The two lenses merge here:
--   NewsAPI  → engagement_score = 0 (no public metrics), mention_type = 'news_article'
--   YouTube  → engagement_score = view_count, mention_type = 'youtube_video'
--
-- This enables queries like: "Show me all brand mentions this week ranked by
-- engagement, regardless of source."
-- =============================================================================

CREATE TABLE IF NOT EXISTS `twolens.brand_mentions` (
  mention_id        STRING      NOT NULL,   -- source-prefixed ID: 'news_{hash}' or 'yt_{video_id}'
  source_platform   STRING      NOT NULL,   -- 'newsapi' | 'youtube'
  source_record_id  STRING      NOT NULL,   -- original article_id or video_id for traceability
  query_term        STRING      NOT NULL,   -- the brand/keyword this mention relates to
  title             STRING      NOT NULL,
  body              STRING,                 -- article content or video description
  author            STRING,                 -- article author or channel name
  url               STRING,                 -- link to original article or video
  published_at      TIMESTAMP,              -- original publish time
  engagement_score  INT64       DEFAULT 0,  -- youtube: view_count | news: 0 (extensible)
  like_count        INT64       DEFAULT 0,  -- youtube: like_count | news: 0
  comment_count     INT64       DEFAULT 0,  -- youtube: comment_count | news: 0
  source_detail     STRING,                 -- news: source_name (e.g. 'BBC News')
                                            -- youtube: channel_title (e.g. 'MKBHD')
  mention_type      STRING,                 -- 'news_article' | 'youtube_video'
  captured_at       TIMESTAMP   NOT NULL,
  pipeline_run_id   STRING      NOT NULL
)
PARTITION BY DATE(captured_at)
CLUSTER BY source_platform, query_term;


-- =============================================================================
-- 4. OBSERVABILITY LAYER: pipeline health and API contract monitoring
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4a. Pipeline runs, one row per execution
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `twolens.pipeline_runs` (
  run_id            STRING      NOT NULL,   -- UUID generated at pipeline start
  started_at        TIMESTAMP   NOT NULL,
  completed_at      TIMESTAMP,              -- NULL if still running or crashed
  trigger_type      STRING      NOT NULL,   -- 'scheduled' | 'manual' | 'retry'
  status            STRING      NOT NULL,   -- 'running' | 'success' | 'partial' | 'failed'
  newsapi_records   INT64       DEFAULT 0,  -- articles fetched from NewsAPI
  youtube_records   INT64       DEFAULT 0,  -- videos fetched from YouTube
  total_loaded      INT64       DEFAULT 0,  -- total rows inserted to brand_mentions
  total_errors      INT64       DEFAULT 0,
  duration_seconds  FLOAT64,                -- wall-clock duration
  quota_used        INT64       DEFAULT 0,  -- YouTube API units consumed this run
  notes             STRING                  -- human-readable summary or error context
)
PARTITION BY DATE(started_at);


-- -----------------------------------------------------------------------------
-- 4b. API errors, granular error tracking
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `twolens.api_errors` (
  error_id          STRING      NOT NULL,   -- UUID
  pipeline_run_id   STRING      NOT NULL,
  api_source        STRING      NOT NULL,   -- 'newsapi' | 'youtube'
  error_type        STRING      NOT NULL,   -- 'timeout' | 'rate_limit' | 'auth_failure'
                                            -- | 'schema_drift' | 'parse_error'
                                            -- | 'validation_error' | 'http_error'
                                            -- | 'quota_exceeded'
  http_status       INT64,                  -- NULL if error occurred before response
  error_message     STRING      NOT NULL,
  request_context   JSON,                   -- sanitized request params (no secrets)
  response_snippet  STRING,                 -- first 1000 chars of unexpected response
  severity          STRING      DEFAULT 'warning',  -- 'info' | 'warning' | 'critical'
  resolved          BOOL        DEFAULT FALSE,
  occurred_at       TIMESTAMP   NOT NULL
)
PARTITION BY DATE(occurred_at)
CLUSTER BY api_source, error_type;


-- -----------------------------------------------------------------------------
-- 4c. API contracts — schema drift detection
-- -----------------------------------------------------------------------------
--
-- On each pipeline run, we hash the structure (top-level keys, nested key paths)
-- of each API response. This table stores the "known good" contract and every
-- detected change. When a drift event fires, the pipeline logs a warning,
-- stores the raw response for analysis, and continues processing fields it
-- can still map (graceful degradation, not hard failure).
--
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `twolens.api_contracts` (
  contract_id       STRING      NOT NULL,   -- UUID
  api_source        STRING      NOT NULL,   -- 'newsapi' | 'youtube'
  endpoint          STRING      NOT NULL,   -- specific endpoint path
  structure_hash    STRING      NOT NULL,   -- SHA-256 of sorted key paths
  structure_keys    STRING,                 -- JSON array of key paths for human review
                                            -- e.g. '["items[].snippet.title",
                                            --        "items[].statistics.viewCount"]'
  first_seen_at     TIMESTAMP   NOT NULL,   -- when this structure was first observed
  last_seen_at      TIMESTAMP   NOT NULL,   -- most recent observation
  is_current        BOOL        DEFAULT TRUE,  -- FALSE once a newer structure is detected
  drift_from        STRING,                 -- contract_id of the previous structure (NULL if first)
  detected_by_run   STRING      NOT NULL    -- pipeline_run_id that first observed this
)
CLUSTER BY api_source, endpoint;


-- =============================================================================
-- QUERY EXAMPLES (for README and demo)
-- =============================================================================

-- Brand mention volume by source over the last 7 days
-- SELECT
--   source_platform,
--   query_term,
--   DATE(captured_at) AS capture_date,
--   COUNT(*) AS mention_count
-- FROM `twolens.brand_mentions`
-- WHERE captured_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
-- GROUP BY source_platform, query_term, capture_date
-- ORDER BY capture_date DESC, mention_count DESC;

-- Top YouTube creators mentioning a brand (by total views)
-- SELECT
--   source_detail AS channel,
--   COUNT(*) AS video_count,
--   SUM(engagement_score) AS total_views,
--   SUM(like_count) AS total_likes,
--   SUM(comment_count) AS total_comments
-- FROM `twolens.brand_mentions`
-- WHERE source_platform = 'youtube'
--   AND query_term = 'your_brand_here'
-- GROUP BY channel
-- ORDER BY total_views DESC
-- LIMIT 10;

-- Media vs. Creator coverage comparison (the "Two Lenses" query)
-- SELECT
--   query_term,
--   COUNTIF(source_platform = 'newsapi') AS news_mentions,
--   COUNTIF(source_platform = 'youtube') AS youtube_mentions,
--   SUM(CASE WHEN source_platform = 'youtube' THEN engagement_score ELSE 0 END) AS youtube_total_views
-- FROM `twolens.brand_mentions`
-- WHERE captured_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
-- GROUP BY query_term
-- ORDER BY news_mentions + youtube_mentions DESC;

-- Detect if any API structure has changed in the last 24 hours
-- SELECT
--   api_source,
--   endpoint,
--   first_seen_at AS drift_detected_at,
--   structure_keys
-- FROM `twolens.api_contracts`
-- WHERE first_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
--   AND drift_from IS NOT NULL;

-- Pipeline health summary
-- SELECT
--   DATE(started_at) AS run_date,
--   COUNT(*) AS total_runs,
--   COUNTIF(status = 'success') AS successful,
--   COUNTIF(status = 'partial') AS partial,
--   COUNTIF(status = 'failed') AS failed,
--   ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
--   SUM(total_loaded) AS total_records_loaded,
--   SUM(quota_used) AS youtube_units_consumed
-- FROM `twolens.pipeline_runs`
-- GROUP BY run_date
-- ORDER BY run_date DESC;

-- YouTube API quota budget check (daily)
-- SELECT
--   DATE(started_at) AS run_date,
--   SUM(quota_used) AS units_used,
--   10000 AS daily_limit,
--   10000 - SUM(quota_used) AS units_remaining,
--   ROUND(SUM(quota_used) / 100.0, 1) AS pct_used
-- FROM `twolens.pipeline_runs`
-- WHERE DATE(started_at) = CURRENT_DATE()
-- GROUP BY run_date;
