#!/usr/bin/env python3
"""
Entry point script for running the py-analysis-worker
"""

import sys
import os

# Add the parent directory (project root) to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.main import main

if __name__ == '__main__':
    main()