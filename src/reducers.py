#!/usr/bin/env python
"""
reducers.py - Unified MapReduce Reducers for all 6 NASA Log Analyses.
Usage in Hadoop Streaming or Terminal:
  python reducers.py hourly        -> Outputs: <hour>\t<count>
  python reducers.py urls          -> Outputs: <url>\t<count> (Top 10)
  python reducers.py status        -> Outputs: <status_code>\t<count>
  python reducers.py ips           -> Outputs: <host>\t<count> (Top 10)
  python reducers.py daily_bytes   -> Outputs: <date>\t<total_bytes>
  python reducers.py daily_errors  -> Outputs: <date>\t<errors>\t<total>\t<error_rate_percent>
"""

import sys
import heapq


def reduce_hourly():
    """Analysis 1: Sum request counts per hour."""
    cur_hour, cur_count = None, 0
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        hour, count_str = parts[0], parts[1]
        try:
            count = int(count_str)
        except ValueError:
            continue

        if cur_hour == hour:
            cur_count += count
        else:
            if cur_hour is not None:
                sys.stdout.write(f"{cur_hour}\t{cur_count}\n")
            cur_hour, cur_count = hour, count

    if cur_hour is not None:
        sys.stdout.write(f"{cur_hour}\t{cur_count}\n")


def reduce_urls(top_n: int = 10):
    """Analysis 2: Sum requests per URL and emit Top 10 via bounded min-heap."""
    cur_url, cur_count = None, 0
    top_heap = []

    def push_heap(url, count):
        if len(top_heap) < top_n:
            heapq.heappush(top_heap, (count, url))
        elif count > top_heap[0][0]:
            heapq.heapreplace(top_heap, (count, url))

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        url, count_str = parts[0], parts[1]
        try:
            count = int(count_str)
        except ValueError:
            continue

        if cur_url == url:
            cur_count += count
        else:
            if cur_url is not None:
                push_heap(cur_url, cur_count)
            cur_url, cur_count = url, count

    if cur_url is not None:
        push_heap(cur_url, cur_count)

    top_records = sorted(top_heap, key=lambda x: (-x[0], x[1]))
    for count, url in top_records:
        sys.stdout.write(f"{url}\t{count}\n")


def reduce_status():
    """Analysis 3: Sum counts per HTTP status code."""
    cur_status, cur_count = None, 0
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        status, count_str = parts[0], parts[1]
        try:
            count = int(count_str)
        except ValueError:
            continue

        if cur_status == status:
            cur_count += count
        else:
            if cur_status is not None:
                sys.stdout.write(f"{cur_status}\t{cur_count}\n")
            cur_status, cur_count = status, count

    if cur_status is not None:
        sys.stdout.write(f"{cur_status}\t{cur_count}\n")


def reduce_ips(top_n: int = 10):
    """Analysis 4: Sum requests per client host/IP and emit Top 10 via min-heap."""
    cur_host, cur_count = None, 0
    top_heap = []

    def push_heap(host, count):
        if len(top_heap) < top_n:
            heapq.heappush(top_heap, (count, host))
        elif count > top_heap[0][0]:
            heapq.heapreplace(top_heap, (count, host))

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        host, count_str = parts[0], parts[1]
        try:
            count = int(count_str)
        except ValueError:
            continue

        if cur_host == host:
            cur_count += count
        else:
            if cur_host is not None:
                push_heap(cur_host, cur_count)
            cur_host, cur_count = host, count

    if cur_host is not None:
        push_heap(cur_host, cur_count)

    top_records = sorted(top_heap, key=lambda x: (-x[0], x[1]))
    for count, host in top_records:
        sys.stdout.write(f"{host}\t{count}\n")


def reduce_daily_bytes():
    """Analysis 5: Sum transferred bytes per calendar date."""
    cur_date, cur_bytes = None, 0
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        date_str, bytes_str = parts[0], parts[1]
        try:
            b_val = int(bytes_str)
        except ValueError:
            continue

        if cur_date == date_str:
            cur_bytes += b_val
        else:
            if cur_date is not None:
                sys.stdout.write(f"{cur_date}\t{cur_bytes}\n")
            cur_date, cur_bytes = date_str, b_val

    if cur_date is not None:
        sys.stdout.write(f"{cur_date}\t{cur_bytes}\n")


def reduce_daily_errors():
    """Analysis 6: Sum errors and total requests; calculate error rate percentage."""
    cur_date, cur_errors, cur_total = None, 0, 0

    def output_date(d_str, errs, tot):
        rate = (errs / tot * 100.0) if tot > 0 else 0.0
        sys.stdout.write(f"{d_str}\t{errs}\t{tot}\t{rate:.4f}\n")

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 3:
            continue
        date_str, err_str, tot_str = parts[0], parts[1], parts[2]
        try:
            is_err = int(err_str)
            tot = int(tot_str)
        except ValueError:
            continue

        if cur_date == date_str:
            cur_errors += is_err
            cur_total += tot
        else:
            if cur_date is not None:
                output_date(cur_date, cur_errors, cur_total)
            cur_date, cur_errors, cur_total = date_str, is_err, tot

    if cur_date is not None:
        output_date(cur_date, cur_errors, cur_total)


REDUCERS = {
    "hourly": reduce_hourly,
    "urls": reduce_urls,
    "status": reduce_status,
    "ips": reduce_ips,
    "daily_bytes": reduce_daily_bytes,
    "daily_errors": reduce_daily_errors,
}


def main():
    job_name = sys.argv[1].lower() if len(sys.argv) > 1 else "hourly"
    reducer_func = REDUCERS.get(job_name)
    if not reducer_func:
        sys.stderr.write(f"Unknown job: {job_name}. Available: {list(REDUCERS.keys())}\n")
        sys.exit(1)
    reducer_func()


if __name__ == "__main__":
    main()
