#!/usr/bin/env python3
"""
Entry point for the Search Keyword Performance application.

Usage:
    python -m src.main <path-to-hit-level-data-file>
"""

import argparse
import logging
import sys
from pathlib import Path

from src.search_keyword_analyzer import SearchKeywordAnalyzer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Parse hit-level data to extract search keyword revenue performance.",
    )
    parser.add_argument("input_file", help="Path to the tab-separated hit-level data file.")
    parser.add_argument(
        "-o", "--output-dir",
        default="output",
        help="Directory for the output .tab file (default: ./output).",
    )
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.is_file():
        log.error("Input file not found: %s", input_path)
        sys.exit(1)

    analyzer = SearchKeywordAnalyzer()

    log.info("Processing %s …", input_path)
    records = analyzer.process_file(input_path)

    if not records:
        log.warning("No search-engine revenue found in the input data.")
    else:
        log.info("Found %d keyword/engine combinations with revenue.", len(records))

    output_path = analyzer.write_output(records, args.output_dir)
    log.info("Results written to %s", output_path)


if __name__ == "__main__":
    main()
