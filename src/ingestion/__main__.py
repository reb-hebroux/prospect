"""CLI entry point: python -m src.ingestion"""

from __future__ import annotations

import logging
import os
import sys

from src.ingestion.bronze import run_bronze_ingest


def main() -> int:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        run_bronze_ingest()
        return 0
    except Exception:
        logging.exception("Bronze ingest failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())