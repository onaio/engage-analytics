#!/usr/bin/env python3
"""Test the logging functionality"""

import json
from datetime import datetime
from pathlib import Path

def log_download(email, ip_address, status='success', error_msg=None):
    """Log download attempts to file"""
    LOG_FILE = Path('/Users/mberg/github/engage-analytics/dataexport/downloads.log')
    LOG_FILE.touch(exist_ok=True)
    
    timestamp = datetime.now().isoformat()
    log_entry = {
        'timestamp': timestamp,
        'email': email,
        'ip_address': ip_address,
        'status': status,
        'error': error_msg
    }
    
    print(f"Writing log entry: {log_entry}")
    
    with open(LOG_FILE, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    print(f"Log written to: {LOG_FILE}")
    
    # Verify it was written
    with open(LOG_FILE, 'r') as f:
        content = f.read()
        print(f"Current log contents:\n{content}")

# Test the logging
log_download("test@example.com", "127.0.0.1", "success")
log_download("user2@example.com", "192.168.1.1", "success")
log_download("failed@example.com", "10.0.0.1", "error", "Export failed")