# NASA Web Server Log Analysis: Student Review & Viva Guide

This single document contains everything you need to understand, run, and explain this Big Data project to your teacher or reviewer.

---

## 1. How to Explain the Project (2-Minute Elevator Pitch)

> **Say this to your teacher:**
>
> *"Sir/Ma'am, in this project we analyze **1.89 million real HTTP web requests** from the NASA Kennedy Space Center server (July 1995) using **Apache Hadoop MapReduce** and **Hadoop Streaming with Python**.*
>
> *Our code in `src/` is structured into 3 clear modules:*
> 1. *`log_parser.py`: Parses each Common Log Format line safely, handles missing bytes (`-` -> `0`), and extracts timestamps, URLs, and status codes.*
> 2. *`mappers.py`: Contains the Map functions for our 6 analyses, emitting tab-separated key-value pairs (like `hour\t1`).*
> 3. *`reducers.py`: Contains the Reduce functions, streaming and aggregating counts per key using $O(1)$ memory and a bounded min-heap for Top-10 queries.*
>
> *We ran all six analyses: hourly traffic, top 10 URLs, HTTP status codes, top 10 client IPs, daily byte volume, and daily error rate. All jobs output standard MapReduce `part-00000` files, clean CSV tables, and charts."*

---

## 2. The 6 Analyses & What the Results Mean

All results below were calculated from the **1,891,714 valid requests** in the official NASA July 1995 dataset:

### Analysis 1: Hourly Request Distribution (00 to 23 EDT)
* **Goal**: Find peak and quiet hours on NASA's server.
* **Peak**: **14:00 EDT (2 PM)** with **122,479 requests**.
* **Trough**: **05:00 EDT (5 AM)** with **31,919 requests**.
* **Insight**: Follows a standard workday curve — traffic rises sharply around 8 AM, peaks in the early afternoon, and drops late at night.
* **Chart**: `charts/hourly_requests.png`

### Analysis 2: Top 10 Most Requested URLs
* **Goal**: Identify the most visited web pages and resources.
* **Top 3**:
  1. `/images/NASA-logosmall.gif` (111,388 requests)
  2. `/images/KSC-logosmall.gif` (89,639 requests)
  3. `/images/MOSAIC-logosmall.gif` (60,468 requests)
* **Insight**: 8 out of the top 10 requests were image icons (`.gif`), proving the workload invariant that multimedia files generate the highest request volume.
* **Chart**: `charts/top_urls.png`

### Analysis 3: HTTP Status Code Breakdown
* **Goal**: Measure server health, cache efficiency, and errors.
* **Results**:
  - `200 OK`: **1,701,534** (89.95%) $\rightarrow$ Successful page loads.
  - `304 Not Modified`: **132,627** (7.01%) $\rightarrow$ Client browser cache hits (saves bandwidth).
  - `302 Found`: **46,573** (2.46%) $\rightarrow$ URL redirects.
  - `404 Not Found`: **10,845** (0.57%) $\rightarrow$ Missing files / broken links.
  - `500 Server Error`: **62** (<0.01%) $\rightarrow$ Internal server failures.
* **Insight**: Over 99.4% of requests succeeded without error, showing very high server stability.
* **Chart**: `charts/status_distribution.png`

### Analysis 4: Top 10 Client Hosts / IPs
* **Goal**: Find which clients or internet service providers sent the most traffic.
* **Top Hosts**:
  1. `piweba3y.prodigy.com` (17,572 requests)
  2. `piweba4y.prodigy.com` (11,591 requests)
  3. `piweba1y.prodigy.com` (9,868 requests)
  4. `alyssa.prodigy.com` (7,852 requests)
* **Insight**: Prodigy (a major 1990s dial-up ISP proxy) accounted for the top 4 spots.
* **Chart**: `charts/top_ips.png`

### Analysis 5: Daily Network Bandwidth Transferred
* **Goal**: Track how much data (bytes) was sent each day.
* **Total month volume**: **38,695,973,491 bytes** (~38.7 GB / 36.04 GiB).
* **Peak Date**: **July 13, 1995** with **2.63 GB** transferred across 134,203 requests.
* **Historical Reason**: July 13 was the launch day of Space Shuttle Discovery (Mission STS-70).
* **Chart**: `charts/daily_bytes.png`

### Analysis 6: Daily HTTP Error Rate (%)
* **Goal**: Track daily failure percentage (HTTP 4xx and 5xx).
* **Formula**:
  $$\text{error\_rate\_percent} = \left(\frac{\text{number of 4xx and 5xx requests}}{\text{total valid requests on that day}}\right) \times 100$$
* **Average**: **0.575%** (always remained below 1.0%).
* **Chart**: `charts/daily_errors.png`

---

## 3. How to Demo the Project in Front of Your Teacher

### Option A: The Web Dashboard (Recommended for Teacher Review)
```bash
python view_dashboard.py
```
*(Or double-click `dashboard/index.html`). Opens an academic light-theme dashboard in your browser with summary cards, step-by-step MapReduce flow, charts, and data tables for all 6 analyses.*

### Option B: The Terminal 30-Second Sample Demo
```bash
python demo_sample.py
```
**Talking Point:**
> *"Sir/Ma'am, here is our live sample demonstration. You can see:
> 1. Raw Common Log Format input lines.
> 2. How our parser extracts host, date, hour, URL, status, and bytes.
> 3. How the Mapper emits key-value pairs (`hour\t1`).
> 4. How Hadoop sorts them.
> 5. How the Reducer computes final aggregates for all 6 analyses."*

### Step 2: Show the Automated Test Suite Passing
```bash
python -m unittest tests/test_all.py
```
*(Runs all 10 unit and integration tests asserting parsing accuracy, missing byte handling (`-`), and MapReduce consistency in 0.01 seconds).*

### Step 3: Run the Full Pipeline on All 1.89M Records
```bash
python run_project.py
```
*(Processes the full dataset, writes standard `part-00000` files, exports CSVs into `outputs/`, saves 6 charts into `charts/`, and verifies data integrity).*

---

## 4. Top 6 Teacher / Viva Questions & Simple Answers

#### Q1: What is the difference between the Mapper and the Reducer?
* **Answer**: The **Mapper** filters and extracts fields from each log line and emits `<key, value>` pairs (e.g., `hour\t1`). The **Reducer** receives all lines with the same key together (because Hadoop sorts them) and sums or aggregates them.

#### Q2: How does Hadoop Streaming work with Python?
* **Answer**: Hadoop runs the Python scripts as subprocesses. It feeds input lines to the Python mapper through standard input (`sys.stdin`), collects tab-separated output from standard output (`sys.stdout`), sorts the keys, and feeds them to the reducer's `sys.stdin`.

#### Q3: How do you handle missing bytes in the Common Log Format?
* **Answer**: When an HTTP status is 304 or 404, the log records a hyphen `-` instead of a number. Our parser detects `-` and treats it as `0` bytes so calculations don't crash.

#### Q4: How does the Top-10 URL Reducer avoid running out of memory?
* **Answer**: Instead of storing all 100,000+ unique URLs in a huge list, the reducer maintains a **min-heap of size 10** (`heapq`). It only keeps the top 10 items in memory at any time.

#### Q5: Did you find any corrupt lines in the dataset?
* **Answer**: Yes, out of 1,891,715 lines, exactly 1 line at the very end (`alyssa.p`) was incomplete. This is historically documented as server disk space running out on July 28, 1995. Our parser safely skips it without crashing.

#### Q6: How did you calculate the daily error rate?
* **Answer**: We divided the number of 4xx and 5xx errors by the total valid requests on that specific day and multiplied by 100. Using the day's total valid requests as the denominator gives an accurate daily percentage.
