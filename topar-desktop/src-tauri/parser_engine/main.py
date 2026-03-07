#!/usr/bin/env python3
"""
Topar Desktop Parser Engine

CLI entry point for product parsing operations.
Communicates with Rust backend via JSON over stdout/stdin.
"""

import argparse
import json
import sys
from typing import Any, Dict

from parser import ProductParser


def log_progress(event: str, data: Dict[str, Any] = None):
    """Send progress update to Rust backend via stdout"""
    message = {
        "type": "progress",
        "event": event,
        "data": data or {}
    }
    print(json.dumps(message), flush=True)


def log_error(error: str, details: Dict[str, Any] = None):
    """Send error message to Rust backend via stdout"""
    message = {
        "type": "error",
        "error": error,
        "details": details or {}
    }
    print(json.dumps(message), flush=True)


def log_result(result: Dict[str, Any]):
    """Send final result to Rust backend via stdout"""
    message = {
        "type": "result",
        "data": result
    }
    print(json.dumps(message), flush=True)


def parse_command(args):
    """Execute parsing operation"""
    parser = None
    try:
        # Create parser instance
        parser = ProductParser(
            source_url=args.source_url,
            limit=args.limit,
            workers=args.workers,
            requests_per_sec=args.requests_per_sec,
            max_sitemaps=args.max_sitemaps,
            progress_callback=log_progress
        )

        # Start parsing
        log_progress("started", {
            "source_url": args.source_url,
            "limit": args.limit,
            "workers": args.workers
        })

        result = parser.parse()

        # Send final result
        log_result(result)

    except KeyboardInterrupt:
        if parser is not None:
            log_result(parser.snapshot_result(
                completed=False,
                error="Parsing cancelled by user"
            ))
        else:
            log_error("Parsing interrupted by user")
        sys.exit(0)
    except Exception as e:
        log_error(str(e), {"type": type(e).__name__})
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Topar product parser engine"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Parse command
    parse_parser = subparsers.add_parser("parse", help="Parse products from URL")
    parse_parser.add_argument(
        "--source-url",
        required=True,
        help="Source URL to parse (sitemap or product page)"
    )
    parse_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of products to parse (0 = unlimited)"
    )
    parse_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers (1-4)"
    )
    parse_parser.add_argument(
        "--requests-per-sec",
        type=float,
        default=3.0,
        help="Maximum requests per second (1.0-20.0)"
    )
    parse_parser.add_argument(
        "--max-sitemaps",
        type=int,
        default=120,
        help="Maximum sitemap files to process"
    )

    args = parser.parse_args()

    if args.command == "parse":
        parse_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
