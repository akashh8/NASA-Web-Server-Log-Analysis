"""
test_all.py - Complete Unified Test Suite for NASA Log Analysis.
Tests:
  1. Common Log Format parsing & edge cases.
  2. All 6 MapReduce pipelines (Hourly, URLs, Status, IPs, Daily Bytes, Daily Errors).
"""

import io
import os
import sys
import unittest

# Ensure project root and src are on sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
for p in [PROJECT_ROOT, SRC_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Imports from src
try:
    from src.log_parser import parse_log_line, extract_url
    from src.mappers import (
        map_hourly, map_urls, map_status, map_ips, map_daily_bytes, map_daily_errors
    )
    from src.reducers import (
        reduce_hourly, reduce_urls, reduce_status, reduce_ips, reduce_daily_bytes, reduce_daily_errors
    )
except ImportError:
    from log_parser import parse_log_line, extract_url
    from mappers import (
        map_hourly, map_urls, map_status, map_ips, map_daily_bytes, map_daily_errors
    )
    from reducers import (
        reduce_hourly, reduce_urls, reduce_status, reduce_ips, reduce_daily_bytes, reduce_daily_errors
    )

FIXTURE_PATH = os.path.join(CURRENT_DIR, "fixtures", "sample_log.txt")


def run_pipeline(mapper_fn, reducer_fn, input_text: str) -> str:
    """Simulate MapReduce: Map -> Sort -> Reduce in memory."""
    # 1. Map step
    old_stdin, old_stdout = sys.stdin, sys.stdout
    map_out = io.StringIO()
    sys.stdin, sys.stdout = io.StringIO(input_text), map_out
    try:
        mapper_fn()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout

    # 2. Shuffle & Sort step
    lines = [l for l in map_out.getvalue().splitlines() if l.strip()]
    lines.sort()
    sorted_input = "\n".join(lines) + ("\n" if lines else "")

    # 3. Reduce step
    reduce_out = io.StringIO()
    sys.stdin, sys.stdout = io.StringIO(sorted_input), reduce_out
    try:
        reducer_fn()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout

    return reduce_out.getvalue()


class TestNASALogAnalysis(unittest.TestCase):

    def setUp(self):
        with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
            self.fixture_data = f.read()

    # --- PART 1: LOG PARSING TESTS ---

    def test_01_valid_record_parsing(self):
        line = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'
        record = parse_log_line(line)
        self.assertIsNotNone(record)
        self.assertEqual(record["host"], "199.72.81.55")
        self.assertEqual(record["date"], "01/Jul/1995")
        self.assertEqual(record["hour"], "00")
        self.assertEqual(record["url"], "/history/apollo/")
        self.assertEqual(record["status"], 200)
        self.assertEqual(record["bytes"], 6245)

    def test_02_missing_bytes_hyphen_handling(self):
        line = 'net-1-141.eden.com - - [01/Jul/1995:14:00:19 -0400] "GET /test.txt HTTP/1.0" 404 -'
        record = parse_log_line(line)
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], 404)
        self.assertEqual(record["bytes"], 0)

    def test_03_malformed_records_skipped(self):
        bad_lines = [
            "",
            "MALFORMED_LINE_WITHOUT_FIELDS",
            'host - - [bad_date] "GET /a" 200 100',
            'alyssa.p',
        ]
        for line in bad_lines:
            self.assertIsNone(parse_log_line(line))

    def test_04_quotes_inside_url(self):
        line = 'frank.mtsu.edu - - [03/Jul/1995:02:41:15 -0400] "GET /images/" HTTP/1.0" 404 -'
        record = parse_log_line(line)
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], 404)

    # --- PART 2: MAPREDUCE ANALYSES TESTS ---

    def test_05_hourly_pipeline(self):
        output = run_pipeline(map_hourly, reduce_hourly, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        hourly_dict = {r[0]: int(r[1]) for r in rows}
        self.assertEqual(hourly_dict.get("00"), 5)

    def test_06_top_urls_pipeline(self):
        output = run_pipeline(map_urls, reduce_urls, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        self.assertLessEqual(len(rows), 10)
        counts = [int(r[1]) for r in rows]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_07_status_code_pipeline(self):
        output = run_pipeline(map_status, reduce_status, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        status_dict = {int(r[0]): int(r[1]) for r in rows}
        self.assertIn(200, status_dict)
        self.assertIn(404, status_dict)

    def test_08_top_ips_pipeline(self):
        output = run_pipeline(map_ips, reduce_ips, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        self.assertLessEqual(len(rows), 10)
        counts = [int(r[1]) for r in rows]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_09_daily_bytes_pipeline(self):
        output = run_pipeline(map_daily_bytes, reduce_daily_bytes, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        bytes_dict = {r[0]: int(r[1]) for r in rows}
        self.assertIn("01/Jul/1995", bytes_dict)
        self.assertGreater(bytes_dict["01/Jul/1995"], 0)

    def test_10_daily_error_rate_pipeline(self):
        output = run_pipeline(map_daily_errors, reduce_daily_errors, self.fixture_data)
        rows = [l.split("\t") for l in output.strip().splitlines() if l]
        for r in rows:
            self.assertEqual(len(r), 4)
            date_str, errors, total, rate = r[0], int(r[1]), int(r[2]), float(r[3])
            expected_rate = (errors / total * 100.0) if total > 0 else 0.0
            self.assertAlmostEqual(rate, expected_rate, places=4)


if __name__ == "__main__":
    unittest.main()
