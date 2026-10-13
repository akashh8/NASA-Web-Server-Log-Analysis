#!/usr/bin/env python
"""
demo_sample.py - Interactive Sample Data & MapReduce Showcase
Use this file to demonstrate the project to your teacher in 30 seconds!

It runs on small, human-readable sample data to clearly illustrate:
  1. What the raw Common Log Format looks like.
  2. How the Mapper parses and emits (key, value) pairs.
  3. How Hadoop Shuffle & Sort groups the keys.
  4. How the Reducer computes final aggregates for all 6 analyses.
"""

import os
import sys
import io

# Setup paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, "src")
FIXTURE_PATH = os.path.join(BASE_DIR, "tests", "fixtures", "sample_log.txt")

sys.path.insert(0, BASE_DIR)
sys.path.insert(0, SRC_DIR)

from src.log_parser import parse_log_line
import src.mappers as mappers
import src.reducers as reducers


def run_pipeline(mapper_func, reducer_func, raw_text: str):
    """Executes Map -> Sort -> Reduce pipeline in memory."""
    old_stdin, old_stdout = sys.stdin, sys.stdout

    # 1. Map Step
    map_out = io.StringIO()
    sys.stdin, sys.stdout = io.StringIO(raw_text), map_out
    try:
        mapper_func()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout

    # 2. Shuffle & Sort Step
    lines = [l for l in map_out.getvalue().splitlines() if l.strip()]
    lines.sort()
    sorted_input = "\n".join(lines) + ("\n" if lines else "")

    # 3. Reduce Step
    red_out = io.StringIO()
    sys.stdin, sys.stdout = io.StringIO(sorted_input), red_out
    try:
        reducer_func()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout

    return map_out.getvalue().splitlines(), lines, red_out.getvalue().splitlines()


def main():
    print("=" * 70)
    print("   NASA HTTP LOG ANALYSIS: LIVE MAPREDUCE SAMPLE DEMO")
    print("=" * 70)

    # 1. Load sample log records
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        sample_lines = [l.strip() for l in f if l.strip()]

    print(f"\n[1] SAMPLE RAW DATA ({len(sample_lines)} test records from Common Log Format):")
    print("-" * 70)
    for i, line in enumerate(sample_lines[:4], 1):
        print(f"  Line {i}: {line}")
    print("  ... [and more sample lines including edge cases like '-' bytes and errors]")

    # 2. Demonstrate Log Parsing
    demo_line = sample_lines[0]
    parsed = parse_log_line(demo_line)
    print("\n[2] HOW OUR PARSER (log_parser.py) EXTRACTS THE FIELDS:")
    print("-" * 70)
    print(f"  Raw Line : {demo_line}")
    print(f"  Host     : {parsed['host']}")
    print(f"  Date     : {parsed['date']}")
    print(f"  Hour     : {parsed['hour']}")
    print(f"  URL      : {parsed['url']}")
    print(f"  Status   : {parsed['status']}")
    print(f"  Bytes    : {parsed['bytes']} bytes (missing '-' is safely parsed as 0)")

    # 3. Step-by-Step MapReduce Execution Example
    print("\n[3] STEP-BY-STEP MAPREDUCE WALKTHROUGH (Example: Hourly Traffic):")
    print("-" * 70)
    raw_sample = "\n".join(sample_lines[:6]) + "\n"
    map_emitted, sorted_pairs, reduce_result = run_pipeline(
        mappers.map_hourly, reducers.reduce_hourly, raw_sample
    )

    print("  Step A (Map Phase)    -> Emits (hour, 1):")
    for pair in map_emitted[:4]:
        k, v = pair.split("\t")
        print(f"     Key='{k}', Value={v}")

    print("\n  Step B (Shuffle/Sort) -> Hadoop sorts all keys in alphabetical order:")
    for pair in sorted_pairs[:4]:
        k, v = pair.split("\t")
        print(f"     Grouped: Key='{k}', Value={v}")

    print("\n  Step C (Reduce Phase) -> Aggregates totals per key:")
    for res in reduce_result:
        k, v = res.split("\t")
        print(f"     -> Hour {k}:00 EDT  ==>  {v} requests")

    # 4. Results of All 6 Analyses on Sample Data
    print("\n" + "=" * 70)
    print("   RESULTS ACROSS ALL 6 ANALYSES ON THE SAMPLE DATASET")
    print("=" * 70)
    full_sample = "\n".join(sample_lines) + "\n"

    # Job 1: Hourly
    _, _, res_hourly = run_pipeline(mappers.map_hourly, reducers.reduce_hourly, full_sample)
    print("\n1. Hourly Request Counts:")
    print("   " + ", ".join([f"{r.split('\t')[0]}h: {r.split('\t')[1]}" for r in res_hourly[:8]]) + " ...")

    # Job 2: Top URLs
    _, _, res_urls = run_pipeline(mappers.map_urls, reducers.reduce_urls, full_sample)
    print("\n2. Top URLs (by request count):")
    for r in res_urls[:3]:
        parts = r.split("\t")
        print(f"   - {parts[0]}: {parts[1]} requests")

    # Job 3: Status Codes
    _, _, res_status = run_pipeline(mappers.map_status, reducers.reduce_status, full_sample)
    print("\n3. HTTP Status Code Distribution:")
    for r in res_status:
        parts = r.split("\t")
        print(f"   - Status {parts[0]}: {parts[1]} requests")

    # Job 4: Top Client IPs
    _, _, res_ips = run_pipeline(mappers.map_ips, reducers.reduce_ips, full_sample)
    print("\n4. Top Client Hosts / IPs:")
    for r in res_ips[:3]:
        parts = r.split("\t")
        print(f"   - {parts[0]}: {parts[1]} requests")

    # Job 5: Daily Bytes
    _, _, res_bytes = run_pipeline(mappers.map_daily_bytes, reducers.reduce_daily_bytes, full_sample)
    print("\n5. Daily Response Bytes:")
    for r in res_bytes:
        parts = r.split("\t")
        print(f"   - {parts[0]}: {int(parts[1]):,} bytes")

    # Job 6: Daily Error Rate
    _, _, res_errors = run_pipeline(mappers.map_daily_errors, reducers.reduce_daily_errors, full_sample)
    print("\n6. Daily Error Rates:")
    for r in res_errors:
        parts = r.split("\t")
        print(f"   - {parts[0]}: {parts[1]} errors / {parts[2]} total requests ({float(parts[3]):.2f}%)")

    print("\n" + "=" * 70)
    print("Demo completed successfully in < 0.1s. Ready for your teacher!")
    print("=" * 70)


if __name__ == "__main__":
    main()
