#!/usr/bin/env python
"""
mappers.py - Unified MapReduce Mappers for all 6 NASA Log Analyses.
Usage in Hadoop Streaming or Terminal:
  python mappers.py hourly        -> Emits: <hour>\t1
  python mappers.py urls          -> Emits: <url>\t1
  python mappers.py status        -> Emits: <status_code>\t1
  python mappers.py ips           -> Emits: <host>\t1
  python mappers.py daily_bytes   -> Emits: <date>\t<bytes>
  python mappers.py daily_errors  -> Emits: <date>\t<is_error>\t1
"""

import sys
import os

# Ensure local imports work reliably
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from log_parser import parse_log_line
except ImportError:
    from src.log_parser import parse_log_line


def map_hourly():
    """Analysis 1: Emit hour and 1."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec:
            sys.stdout.write(f"{rec['hour']}\t1\n")


def map_urls():
    """Analysis 2: Emit URL and 1."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec and rec["url"]:
            sys.stdout.write(f"{rec['url']}\t1\n")


def map_status():
    """Analysis 3: Emit HTTP status code and 1."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec:
            sys.stdout.write(f"{rec['status']}\t1\n")


def map_ips():
    """Analysis 4: Emit client host/IP and 1."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec and rec["host"]:
            sys.stdout.write(f"{rec['host']}\t1\n")


def map_daily_bytes():
    """Analysis 5: Emit calendar date and response bytes."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec:
            sys.stdout.write(f"{rec['date']}\t{rec['bytes']}\n")


def map_daily_errors():
    """Analysis 6: Emit date, is_error (1 for 4xx/5xx else 0), and total count 1."""
    for line in sys.stdin:
        rec = parse_log_line(line)
        if rec:
            is_err = 1 if 400 <= rec["status"] <= 599 else 0
            sys.stdout.write(f"{rec['date']}\t{is_err}\t1\n")


MAPPERS = {
    "hourly": map_hourly,
    "urls": map_urls,
    "status": map_status,
    "ips": map_ips,
    "daily_bytes": map_daily_bytes,
    "daily_errors": map_daily_errors,
}


def main():
    job_name = sys.argv[1].lower() if len(sys.argv) > 1 else "hourly"
    mapper_func = MAPPERS.get(job_name)
    if not mapper_func:
        sys.stderr.write(f"Unknown job: {job_name}. Available: {list(MAPPERS.keys())}\n")
        sys.exit(1)
    mapper_func()


if __name__ == "__main__":
    main()
