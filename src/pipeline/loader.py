"""
TwoLens BigQuery Loader
──────────────────────────
Handles all BigQuery operations: table creation, row inserts, and
pipeline run lifecycle (start/complete). Each table has its own
insert method so callers don't need to know table IDs or schemas.

Design:
  - All inserts use insert_rows_json (streaming insert) for simplicity.
    On free tier with low volume this is fine. For production scale,
    you'd switch to load jobs for batches.
  - Errors during insert are logged and returned, never raised.
    A failed insert to one table shouldn't kill the whole pipeline.
"""

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from google.cloud import bigquery

from src.config import Config

log = logging.getLogger(__name__)


class BigQueryLoader:
    """Manages BigQuery client and all insert operations for the pipeline."""

    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.gcp_project_id)
        self.dataset = config.bq_dataset

    def _table_id(self, table_name: str) -> str:
        """Fully qualified table ID: project.dataset.table"""
        return f"{self.config.gcp_project_id}.{self.dataset}.{table_name}"

    def _insert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> list[dict]:
        """
        Insert rows into a BigQuery table via streaming insert.

        Returns a list of errors (empty list = success). Each error is a dict
        with 'index' and 'errors' keys from the BigQuery API.
        """
        if not rows:
            log.info(f"BigQuery: No rows to insert into {table_name}")
            return []

        table_id = self._table_id(table_name)

        try:
            errors = self.client.insert_rows_json(table_id, rows)
            if errors:
                log.error(f"BigQuery: {len(errors)} insert errors in {table_name}: {errors}")
            else:
                log.info(f"BigQuery: Inserted {len(rows)} rows into {table_name}")
            return errors
        except Exception as e:
            log.error(f"BigQuery: Failed to insert into {table_name}: {e}")
            return [{"error": str(e)}]

    # ─── Table-specific inserts ───────────────────────────────────────────

    def insert_raw_responses(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("raw_api_responses", rows)

    def insert_news_articles(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("news_articles", rows)

    def insert_youtube_videos(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("youtube_videos", rows)

    def insert_brand_mentions(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("brand_mentions", rows)

    def insert_api_errors(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("api_errors", rows)

    def insert_api_contracts(self, rows: list[dict[str, Any]]) -> list[dict]:
        return self._insert_rows("api_contracts", rows)

    # ─── Contract Loading (Drift Detection) ───────────────────────────────

    def load_known_contracts(self) -> dict[str, str]:
        """
        Load the most recent contract hash for each (api_source, endpoint) pair.

        Returns a dict mapping "api_source:endpoint" -> structure_hash,
        which is the format check_drift() expects. This means drift detection
        compares against the LAST RUN's contracts, not an empty dict.

        If the query fails (table doesn't exist yet, permissions, etc.),
        returns an empty dict so the pipeline still runs — it just treats
        everything as a first observation.
        """

        query = f"""
            SELECT api_source, endpoint, structure_hash
            FROM `{self._table_id("api_contracts")}`
            WHERE is_current = TRUE
        """  # nosec B608

        try:
            results = self.client.query(query).result()
            known: dict[str, str] = {}
            for row in results:
                lookup_key = f"{row.api_source}:{row.endpoint}"
                known[lookup_key] = row.structure_hash

            log.info(f"BigQuery: Loaded {len(known)} known API contracts for drift detection")
            return known

        except Exception as e:
            log.warning(f"BigQuery: Could not load API contracts (table may not exist yet): {e}")
            return {}

    # ─── Pipeline Run Lifecycle ───────────────────────────────────────────

    def start_run(self, trigger_type: str = "manual") -> str:
        """
        Create a new pipeline_runs row in 'running' state.
        Returns the generated run_id.
        """
        run_id = uuid.uuid4().hex
        row = {
            "run_id": run_id,
            "started_at": datetime.now(UTC).isoformat(),
            "completed_at": None,
            "trigger_type": trigger_type,
            "status": "running",
            "newsapi_records": 0,
            "youtube_records": 0,
            "total_loaded": 0,
            "total_errors": 0,
            "duration_seconds": None,
            "quota_used": 0,
            "notes": None,
        }
        self._insert_rows("pipeline_runs", [row])
        log.info(f"Pipeline run started: {run_id}")
        return run_id

    def complete_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        newsapi_records: int = 0,
        youtube_records: int = 0,
        total_loaded: int = 0,
        total_errors: int = 0,
        quota_used: int = 0,
        notes: str | None = None,
    ) -> None:
        """
        Insert a completion row for the pipeline run.

        Note: BigQuery streaming inserts don't support UPDATE, so we insert
        a new row with the final state. For reporting, queries should use
        the row with the latest completed_at for each run_id, or we can
        deduplicate with a view. This is common practice for OLAP dbs.
        """
        now = datetime.now(UTC)
        duration = (now - started_at).total_seconds()

        row = {
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "completed_at": now.isoformat(),
            "trigger_type": "scheduled",  # will be overwritten by caller if needed
            "status": status,
            "newsapi_records": newsapi_records,
            "youtube_records": youtube_records,
            "total_loaded": total_loaded,
            "total_errors": total_errors,
            "duration_seconds": round(duration, 2),
            "quota_used": quota_used,
            "notes": notes,
        }
        self._insert_rows("pipeline_runs", [row])
        log.info(
            f"Pipeline run completed: {run_id} | status={status} | "
            f"loaded={total_loaded} | errors={total_errors} | "
            f"duration={duration:.1f}s"
        )

    # ─── Schema Bootstrap ─────────────────────────────────────────────────

    def ensure_tables_exist(self) -> None:
        """
        Create all TwoLens tables if they don't already exist.
        Reads schema.sql and executes each CREATE TABLE statement.
        This makes the repo self-contained — no manual setup needed.
        """
        from pathlib import Path

        schema_path = Path(__file__).resolve().parent.parent.parent / "schema.sql"
        if not schema_path.exists():
            log.warning(f"schema.sql not found at {schema_path} — skipping table creation")
            return

        sql = schema_path.read_text()

        # Split on CREATE TABLE and re-add the prefix
        statements = [s.strip() for s in sql.split("CREATE TABLE") if s.strip()]
        created = 0

        for stmt in statements:
            # Skip comment-only chunks
            if not stmt.startswith("IF NOT EXISTS"):
                continue

            full_stmt = f"CREATE TABLE {stmt}"
            full_stmt = full_stmt.replace("`twolens.", f"`{self.dataset}.")

            # Strip trailing comments (everything after the closing paren + semicolon)
            try:
                self.client.query(full_stmt).result()
                created += 1
            except Exception as e:
                # Table likely already exists or syntax issue, log and continue
                log.debug(f"Table creation note: {e}")

        log.info(f"BigQuery: Ensured {created} tables exist in {self.dataset}")
