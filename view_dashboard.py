#!/usr/bin/env python
"""
view_dashboard.py - Instant 1-Click Launcher for the NASA Log Analysis Dashboard.
Opens the interactive web frontend in your default browser.
"""

import os
import webbrowser

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "dashboard", "index.html")
    
    if os.path.exists(html_path):
        print("=" * 60)
        print("Launching NASA Big Data MapReduce Dashboard...")
        print(f"File: {html_path}")
        print("=" * 60)
        webbrowser.open(f"file:///{html_path.replace(os.sep, '/')}")
    else:
        print(f"Error: {html_path} not found.")

if __name__ == "__main__":
    main()
