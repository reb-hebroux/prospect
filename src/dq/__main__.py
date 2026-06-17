"""CLI entry point: python -m src.dq"""

from __future__ import annotations

import sys

from src.dq.runner import main

if __name__ == "__main__":
    sys.exit(main())