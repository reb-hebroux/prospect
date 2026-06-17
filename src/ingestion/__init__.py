"""Bronze layer — raw ingest and schema enforcement (Step 2)."""

from src.ingestion.bronze import run_bronze_ingest

__all__ = ["run_bronze_ingest"]