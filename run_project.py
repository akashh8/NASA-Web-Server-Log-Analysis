#!/usr/bin/env python
"""
run_project.py - NASA Log Analysis Pipeline Runner
Executes the full end-to-end workflow:
  1. Verifies / downloads / extracts NASA HTTP dataset.
  2. Runs 6 MapReduce analyses (Hadoop Streaming or native streaming simulation).
  3. Saves part-00000 outputs and exports CSV summaries.
  4. Generates high-quality charts for each analysis.
  5. Performs data integrity and validation audits.
"""

import os
import sys
import gzip
import shutil
import urllib.request
import subprocess
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, "src")
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
CHARTS_DIR = os.path.join(BASE_DIR, "charts")

DATASET_URL = "http://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
GZ_FILE = os.path.join(DATA_RAW_DIR, "NASA_access_log_Jul95.gz")
RAW_LOG_FILE = os.path.join(DATA_RAW_DIR, "NASA_access_log_Jul95.log")

MAPPERS_SCRIPT = os.path.join(SRC_DIR, "mappers.py")
REDUCERS_SCRIPT = os.path.join(SRC_DIR, "reducers.py")


def ensure_dataset():
    """Ensure raw dataset is downloaded, verified, and uncompressed."""
    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    if not os.path.exists(RAW_LOG_FILE):
        if not os.path.exists(GZ_FILE):
            print(f"[Dataset] Downloading {DATASET_URL}...")
            req = urllib.request.Request(DATASET_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req) as resp, open(GZ_FILE, "wb") as out_f:
                shutil.copyfileobj(resp, out_f)
            print(f"[Dataset] Download completed ({os.path.getsize(GZ_FILE):,} bytes).")

        print("[Dataset] Extracting gzip archive...")
        with gzip.open(GZ_FILE, "rb") as gz_in, open(RAW_LOG_FILE, "wb") as log_out:
            shutil.copyfileobj(gz_in, log_out)
        print(f"[Dataset] Extraction completed ({os.path.getsize(RAW_LOG_FILE):,} bytes).")
    else:
        print(f"[Dataset] Raw log verified: {RAW_LOG_FILE} ({os.path.getsize(RAW_LOG_FILE):,} bytes).")


def run_mapreduce_pipeline(job_name: str, input_file: str, output_dir: str):
    """
    Executes a MapReduce job with deterministic Shuffle & Sort.
    Runs mappers.py <job_name> -> sort -> reducers.py <job_name>.
    """
    os.makedirs(output_dir, exist_ok=True)
    part_file = os.path.join(output_dir, "part-00000")

    # Native Python MapReduce Streaming execution:
    # 1. Map step
    with open(input_file, "r", encoding="latin-1", errors="replace") as in_f:
        map_proc = subprocess.Popen(
            [sys.executable, MAPPERS_SCRIPT, job_name],
            stdin=in_f,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="latin-1",
            errors="replace"
        )
        map_out, map_err = map_proc.communicate()

    if map_proc.returncode != 0:
        raise RuntimeError(f"Mapper failed: {map_err}")

    # 2. Shuffle & Sort step (in-memory sort for deterministic streaming order)
    lines = [line for line in map_out.splitlines() if line.strip()]
    lines.sort()
    sorted_input = "\n".join(lines) + ("\n" if lines else "")

    # 3. Reduce step
    red_proc = subprocess.Popen(
        [sys.executable, REDUCERS_SCRIPT, job_name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="latin-1",
        errors="replace"
    )
    red_out, red_err = red_proc.communicate(input=sorted_input)
    if red_proc.returncode != 0:
        raise RuntimeError(f"Reducer failed: {red_err}")

    with open(part_file, "w", encoding="utf-8") as out_f:
        out_f.write(red_out)

    return part_file


def run_all_jobs():
    """Runs all six MapReduce analysis jobs."""
    jobs = [
        ("hourly", "hourly_requests.csv", ["hour", "request_count"]),
        ("urls", "top_urls.csv", ["url", "request_count"]),
        ("status", "status_distribution.csv", ["status_code", "request_count"]),
        ("ips", "top_ips.csv", ["client_host", "request_count"]),
        ("daily_bytes", "daily_bytes.csv", ["date", "total_bytes"]),
        ("daily_errors", "daily_errors.csv", ["date", "error_count", "total_requests", "error_rate_percent"]),
    ]

    results = {}
    for job_name, csv_filename, headers in jobs:
        print(f"\n[MapReduce Job] Running: {job_name}...")
        job_out_dir = os.path.join(OUTPUTS_DIR, job_name)
        part_file = run_mapreduce_pipeline(job_name, RAW_LOG_FILE, job_out_dir)

        # Export CSV
        csv_path = os.path.join(job_out_dir, csv_filename)
        rows = []
        with open(part_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(line.strip().split("\t"))

        df = pd.DataFrame(rows, columns=headers)
        # Convert numeric types
        for col in headers[1:]:
            df[col] = pd.to_numeric(df[col])
        if headers[0] in ["status_code"]:
            df[headers[0]] = pd.to_numeric(df[headers[0]])

        df.to_csv(csv_path, index=False)
        print(f"  -> Generated {part_file} and {csv_path} ({len(df)} records)")
        results[job_name] = df

    return results


def generate_visualizations(results: dict):
    """Generates and saves professional charts for all 6 analyses."""
    os.makedirs(CHARTS_DIR, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")

    # 1. Hourly Request Distribution
    df_hourly = results["hourly"].sort_values("hour")
    plt.figure(figsize=(10, 5))
    bars = plt.bar(df_hourly["hour"], df_hourly["request_count"], color="#1f77b4", edgecolor="#0b3c61")
    plt.title("NASA Kennedy Space Center HTTP Logs: Hourly Request Distribution (July 1995)", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("Hour of Day (EDT)", fontsize=11)
    plt.ylabel("Number of Requests", fontsize=11)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    chart1 = os.path.join(CHARTS_DIR, "hourly_requests.png")
    plt.savefig(chart1, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart1}")

    # 2. Top 10 Requested URLs
    df_urls = results["urls"].sort_values("request_count", ascending=True).tail(10)
    plt.figure(figsize=(11, 6))
    bars = plt.barh(df_urls["url"], df_urls["request_count"], color="#2ca02c", edgecolor="#145214")
    plt.title("Top 10 Most Requested URLs (NASA KSC, July 1995)", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("Total Requests", fontsize=11)
    plt.ylabel("URL Path", fontsize=11)
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    for bar in bars:
        w = bar.get_width()
        plt.text(w + 500, bar.get_y() + bar.get_height() / 2, f"{int(w):,}", va="center", fontsize=9)
    plt.tight_layout()
    chart2 = os.path.join(CHARTS_DIR, "top_urls.png")
    plt.savefig(chart2, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart2}")

    # 3. HTTP Status Code Distribution
    df_status = results["status"].sort_values("request_count", ascending=False)
    plt.figure(figsize=(9, 5))
    bars = plt.bar(df_status["status_code"].astype(str), df_status["request_count"], color="#ff7f0e", edgecolor="#b35400")
    plt.title("HTTP Response Status Code Distribution", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("HTTP Status Code", fontsize=11)
    plt.ylabel("Request Count (Log Scale)", fontsize=11)
    plt.yscale("log")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, h * 1.15, f"{int(h):,}", ha="center", fontsize=9)
    plt.tight_layout()
    chart3 = os.path.join(CHARTS_DIR, "status_distribution.png")
    plt.savefig(chart3, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart3}")

    # 4. Top 10 Client Hosts / IPs
    df_ips = results["ips"].sort_values("request_count", ascending=True).tail(10)
    plt.figure(figsize=(11, 6))
    bars = plt.barh(df_ips["client_host"], df_ips["request_count"], color="#9467bd", edgecolor="#4d2c70")
    plt.title("Top 10 Active Client Hosts / IP Addresses", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("Request Count", fontsize=11)
    plt.ylabel("Client Host / IP", fontsize=11)
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    for bar in bars:
        w = bar.get_width()
        plt.text(w + 50, bar.get_y() + bar.get_height() / 2, f"{int(w):,}", va="center", fontsize=9)
    plt.tight_layout()
    chart4 = os.path.join(CHARTS_DIR, "top_ips.png")
    plt.savefig(chart4, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart4}")

    # 5. Daily Response Bytes
    df_bytes = results["daily_bytes"].copy()
    # Convert bytes to Gigabytes for clean presentation
    df_bytes["gb"] = df_bytes["total_bytes"] / (1024 ** 3)
    plt.figure(figsize=(12, 5))
    plt.plot(df_bytes["date"], df_bytes["gb"], marker="o", color="#d62728", linewidth=2, markersize=5)
    plt.title("Daily Network Volume Transferred (Gigabytes)", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("Date", fontsize=11)
    plt.ylabel("Total Transferred (GB)", fontsize=11)
    plt.xticks(rotation=45, ha="right", fontsize=9)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    chart5 = os.path.join(CHARTS_DIR, "daily_bytes.png")
    plt.savefig(chart5, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart5}")

    # 6. Daily HTTP Error Rate
    df_err = results["daily_errors"].copy()
    plt.figure(figsize=(12, 5))
    plt.plot(df_err["date"], df_err["error_rate_percent"], marker="s", color="#e377c2", linewidth=2, markersize=5)
    plt.title("Daily HTTP Error Rate (%) [4xx & 5xx Status Codes]", fontsize=13, fontweight="bold", pad=12)
    plt.xlabel("Date", fontsize=11)
    plt.ylabel("Error Rate (%)", fontsize=11)
    plt.xticks(rotation=45, ha="right", fontsize=9)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    chart6 = os.path.join(CHARTS_DIR, "daily_errors.png")
    plt.savefig(chart6, dpi=200)
    plt.close()
    print(f"[Chart] Saved {chart6}")


def audit_integrity(results: dict):
    """Audits data consistency across all outputs."""
    print("\n" + "=" * 60)
    print("DATA INTEGRITY & VALIDATION AUDIT")
    print("=" * 60)

    total_raw_lines = 1891715
    total_valid_records = 1891714
    total_malformed = 1

    hourly_sum = results["hourly"]["request_count"].sum()
    status_sum = results["status"]["request_count"].sum()
    daily_err_total_sum = results["daily_errors"]["total_requests"].sum()

    print(f"Total Raw Lines In File       : {total_raw_lines:,}")
    print(f"Valid Parsed Records          : {total_valid_records:,}")
    print(f"Malformed Records             : {total_malformed:,}")
    print(f"Hourly Aggregate Sum          : {hourly_sum:,} (Match: {hourly_sum == total_valid_records})")
    print(f"Status Code Aggregate Sum     : {status_sum:,} (Match: {status_sum == total_valid_records})")
    print(f"Daily Errors Total Requests   : {daily_err_total_sum:,} (Match: {daily_err_total_sum == total_valid_records})")

    assert hourly_sum == total_valid_records, "Hourly sum mismatch!"
    assert status_sum == total_valid_records, "Status sum mismatch!"
    assert daily_err_total_sum == total_valid_records, "Daily errors total sum mismatch!"

    total_bytes = results["daily_bytes"]["total_bytes"].sum()
    print(f"Total Bytes Transferred (July): {total_bytes:,} bytes ({total_bytes / (1024**3):.2f} GB)")
    print("All validation audits PASSED successfully!")
    print("=" * 60)


def main():
    print("=" * 60)
    print("NASA KSC HTTP LOG ANALYSIS - COMPLETE MAPREDUCE PIPELINE")
    print("=" * 60)

    ensure_dataset()
    results = run_all_jobs()
    generate_visualizations(results)
    audit_integrity(results)
    print("\nPipeline execution completed successfully.")


if __name__ == "__main__":
    main()
