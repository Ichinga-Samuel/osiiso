"""Run the Hacker News showcase.

Usage:
    python -m examples.hackernews_showcase --limit 6
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from pathlib import Path

import osiiso

from .workflows import print_report, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the osiiso Hacker News showcase.")
    parser.add_argument("--limit", type=int, default=6, help="Number of story/job/poll items to fetch.")
    parser.add_argument("--database", type=Path, default=Path(tempfile.gettempdir()) / "osiiso_hn_showcase.sqlite3")
    parser.add_argument("--online", action="store_true", help="Use the live Hacker News API instead of fixtures.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    result = osiiso.run(run_pipeline(args.limit, database=args.database, offline=not args.online))
    print_report(result)


if __name__ == "__main__":
    main()
