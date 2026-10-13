"""
log_parser.py - Robust Common Log Format (CLF) Parser for NASA HTTP Logs.
"""

import re
from typing import Optional, Dict, Any

# CLF pattern: host identd authuser [date:hour:minute:second timezone] "request" status bytes
CLF_PATTERN = re.compile(
    r'^(\S+)\s+\S+\s+\S+\s+\[([^:]+):(\d{2}):(\d{2}):(\d{2})\s+([^\]]+)\]\s+"(.*)"\s+(\d{3})\s+(\S+)$'
)

HTTP_METHODS = {"GET", "POST", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE", "CONNECT"}


def extract_url(request_str: str) -> str:
    """Extract clean URL path from HTTP request string."""
    if not request_str:
        return "-"
    parts = request_str.strip().split()
    if len(parts) >= 2 and parts[0].upper() in HTTP_METHODS:
        return parts[1]
    elif len(parts) >= 1:
        if parts[0].upper() in HTTP_METHODS:
            return "-"
        return parts[0]
    return "-"


def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse one raw line. Returns dict of parsed fields or None if malformed."""
    if not line:
        return None

    cleaned_line = line.strip()
    match = CLF_PATTERN.match(cleaned_line)
    if not match:
        return None

    host = match.group(1)
    date_str = match.group(2)
    hour = match.group(3)
    minute = match.group(4)
    second = match.group(5)
    tz = match.group(6)
    request_str = match.group(7)
    status_str = match.group(8)
    bytes_str = match.group(9)

    try:
        status = int(status_str)
    except ValueError:
        return None

    # Handle missing/zero byte marker '-'
    if bytes_str == "-":
        byte_count = 0
    else:
        try:
            byte_count = int(bytes_str)
            if byte_count < 0:
                byte_count = 0
        except ValueError:
            return None

    url = extract_url(request_str)

    req_parts = request_str.strip().split()
    method = req_parts[0].upper() if req_parts and req_parts[0].upper() in HTTP_METHODS else None
    protocol = req_parts[-1] if len(req_parts) >= 3 and req_parts[-1].startswith("HTTP/") else None

    return {
        "host": host,
        "date": date_str,
        "hour": hour,
        "minute": minute,
        "second": second,
        "timezone": tz,
        "timestamp": f"{date_str}:{hour}:{minute}:{second} {tz}",
        "request": request_str,
        "method": method,
        "url": url,
        "protocol": protocol,
        "status": status,
        "bytes": byte_count,
        "raw_bytes": bytes_str,
    }
