"""CLI entry point: python -m src.transform"""

from __future__ import annotations

import logging
import os
import sys

from src.transform.silver import run_silver_transform


def main() -> int:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        run_silver_transform()
        return 0
    except Exception:
        logging.exception("Silver transform failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())