# Web Server Log Analysis Using Hadoop MapReduce
### A Case Study on the NASA Kennedy Space Center HTTP Logs (July 1995)

---

## 1. What exactly is our project?
Our project title is:
**Web Server Log Analysis Using Hadoop MapReduce: A Case Study on the NASA Kennedy Space Center HTTP Logs**

In simple words, we take a large file containing records of requests made to a NASA web server and use Hadoop MapReduce to analyse those requests automatically.

Imagine that thousands of people visit a website. Every time someone requests a page, image, or other resource, the web server records information about that request in a log file.

After a long period, this file becomes very large. Instead of manually reading millions of lines, we write programs that count and summarize the information we need.

Our project answers six questions:
1. When does the website receive the most requests?
2. Which pages or resources are requested most often?
3. Which HTTP status codes occur most frequently?
4. Which client IP addresses generate the most requests?
5. How many response bytes does the server send each day?
6. What percentage of requests result in HTTP 4xx or 5xx responses each day?

We use Hadoop MapReduce to calculate these results and Python visualizations to present them.

---

## 2. What problem are we solving?
Consider this small example of a web server log:

```text
192.168.1.10 - - [01/Jul/1995:10:15:00 -0400] "GET /index.html HTTP/1.0" 200 1500
192.168.1.11 - - [01/Jul/1995:10:16:00 -0400] "GET /images/logo.gif HTTP/1.0" 200 2500
192.168.1.10 - - [01/Jul/1995:10:17:00 -0400] "GET /missing.html HTTP/1.0" 404 300
```

Each line describes one HTTP request. From these three lines, we can determine that:
- `/index.html` was requested once.
- `/images/logo.gif` was requested once.
- `/missing.html` was requested once, but returned a 404 response.
- Two requests returned HTTP 200.
- One request returned HTTP 404.
- The server sent a total of 4,300 response bytes.

These calculations are simple for three records. However, our NASA dataset contains approximately 1.9 million log lines (205 MB), so manually performing these calculations is impossible.

Our solution is to automate the extraction, counting, aggregation, and visualization of these patterns using MapReduce.

---

## 3. What is our dataset?
- **Dataset**: NASA-HTTP access log (July 1995)
- **Source**: Internet Traffic Archive (ITA), hosted by Lawrence Berkeley National Laboratory ([Dataset Link](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html))
- **Server**: NASA Kennedy Space Center's WWW server
- **Period**: 01/Jul/1995 through 28/Jul/1995
- **Total lines in file**: 1,891,715 lines (1,891,714 valid records + 1 truncated EOF line `alyssa.p`)
- **Format**: Plain-text NCSA Common Log Format

The dataset was collected by Jim Dumoulin and contributed by Martin Arlitt and Carey Williamson. It is a historical dataset used in web server workload research.

*A key distinction*: this is historical log analysis, not a live NASA monitoring system. We are analysing recorded requests from July 1995, not collecting current NASA traffic.

---

## 4. What information does each log line contain?
Example:
```text
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

1. **Client IP address (`199.72.81.55`)**: Identifies the recorded host that made the request.
2. **Timestamp (`01/Jul/1995:00:00:01 -0400`)**: Tells us the date, time, and timezone of the request.
3. **HTTP request (`GET /history/apollo/ HTTP/1.0`)**: Contains the method, requested resource, and HTTP protocol version.
4. **Status code (`200`)**: Indicates the HTTP response status (200 = Success).
5. **Response size (`6245`)**: Records the response size in bytes (a `-` means 0 bytes transferred).

Our parser (`src/log_parser.py`) extracts these fields from every valid log record. If a line does not match the expected format, it is safely skipped rather than crashing the program.

---

## 5. Why are we using Hadoop MapReduce?
Hadoop is a framework for processing large datasets. MapReduce is its programming model for converting raw input records into aggregated results.

Suppose we want to count how many times each HTTP status code occurs:
`200, 404, 200, 500, 200`

- **Step 1: Mapper**
  For each request, the mapper extracts the status code and emits:
  `200 1`, `404 1`, `200 1`, `500 1`, `200 1`
- **Step 2: Shuffle and Sort**
  Hadoop automatically groups values belonging to the same key:
  `200 [1, 1, 1]`, `404 [1]`, `500 [1]`
- **Step 3: Reducer**
  The reducer adds up the values:
  `200 3`, `404 1`, `500 1`

For this project, Hadoop Streaming lets us write the mapper and reducer programs in Python (`src/mappers.py` and `src/reducers.py`) while Hadoop handles the MapReduce processing.

*Note*: Our notebook runs Hadoop in local mode to demonstrate the MapReduce model cleanly without needing a costly physical cluster.

---

## 6. What exactly are our six analyses?

### Analysis 1: Hits per hour
- **Question**: At which hours does the server receive the most requests?
- **Result**: Traffic peaks at **14:00 EDT (2 PM)** with **122,479 requests**, and hits its lowest point at **05:00 EDT (5 AM)** with **31,919 requests**.

### Analysis 2: Most requested URLs
- **Question**: Which resources were requested most often?
- **Result**: Top resource is `/images/NASA-logosmall.gif` (111,388 requests). 8 out of the top 10 URLs are image files (`.gif`), showing graphics generate the most traffic.

### Analysis 3: HTTP status-code counts
- **Question**: What responses did the server return?
- **Result**: Over **89.95%** were `200 OK` (1.70M) and **7.01%** were `304 Not Modified` (browser cache hits). Errors were only **0.58%**.

### Analysis 4: Top client IP addresses
- **Question**: Which recorded client hosts generated the most requests?
- **Result**: Prodigy dial-up ISP web proxies (`piweba3y.prodigy.com`, `piweba4y`, `piweba1y`, `alyssa`) contributed the largest share of requests.

### Analysis 5: Daily response bytes
- **Question**: How many response bytes did the server send each day?
- **Result**: Total bandwidth transferred across July was **36.04 GB (38.70 GB)**. Peak day was **July 13, 1995 (2.63 GB)**, which was the launch day of Space Shuttle Discovery on mission STS-70.

### Analysis 6: Daily HTTP error rate
- **Question**: What percentage of requests each day returned HTTP status codes in the 400-599 range?
- **Formula**:
  $$\text{Error rate} = \left(\frac{\text{Number of 4xx and 5xx requests}}{\text{Total valid requests for that date}}\right) \times 100$$
- **Result**: Average daily error rate was **0.575%** (consistently below 1.0% across the month).

---

## 7. How do all the components fit together?
1. **Dataset acquisition**: The dataset (`NASA_access_log_Jul95.gz`) is downloaded and extracted into `data/raw/`.
2. **Parsing & validation**: `src/log_parser.py` safely extracts fields from each line.
3. **MapReduce processing**: `src/mappers.py` and `src/reducers.py` process the 6 analyses.
4. **Outputs & CSVs**: Results are stored in `outputs/` as `part-00000` files and exported as clean `.csv` files.
5. **Visualizations**: Charts are generated and saved in `charts/`.

---

## 8. Which technologies are we using?
- **NASA HTTP dataset**: Real historical log records from July 1995.
- **Python**: Implements the parser, mappers, reducers, charts, and demo script.
- **Java 11**: Runtime required for Hadoop.
- **Apache Hadoop 3.4.1**: Provides the MapReduce streaming framework.
- **Pandas & Matplotlib**: Generates the CSV files and charts.
- **Google Colab**: Cloud execution environment for Hadoop Streaming.

---

## 9. Quick Commands to Run for the Review

### 1. Open the Simple Web Dashboard (Best for Mam)
```bash
python view_dashboard.py
```
*(Or double-click `dashboard/index.html` in your browser).*

### 2. Run the 30-Second Live Terminal Demo
```bash
python demo_sample.py
```

### 3. Run the Automated Tests (Takes 0.01 seconds)
```bash
python -m unittest tests/test_all.py
```

### 4. Run the Full MapReduce Pipeline on All 1.89M Records
```bash
python run_project.py
```

---

## 10. Simplified Project Structure
```
nasa-log-analysis/
├── README.md                      # This project overview
├── requirements.txt               # Dependencies (pandas, matplotlib, pytest)
├── run_project.py                 # Master pipeline runner
├── demo_sample.py                 # 30-second live terminal demo
├── view_dashboard.py              # Opens web dashboard in browser
├── notebooks/
│   └── NASA_Log_Analysis.ipynb    # Google Colab notebook (Java 11 + Hadoop 3.4.1)
├── src/                           # 3 core files
│   ├── log_parser.py              # Common Log Format parser
│   ├── mappers.py                 # All 6 mappers
│   └── reducers.py                # All 6 reducers
├── tests/
│   ├── test_all.py                # Single test file (10 passing tests)
│   └── fixtures/sample_log.txt    # Small test log
├── data/raw/                      # NASA_access_log_Jul95.log
├── outputs/                       # 6 folders with part-00000 and .csv files
├── charts/                        # 6 generated chart images (PNG)
├── dashboard/
│   └── index.html                 # Clean academic light-theme dashboard
└── docs/
    └── PROJECT_REVIEW_GUIDE.md    # Review notes and teacher viva Q&A
```
