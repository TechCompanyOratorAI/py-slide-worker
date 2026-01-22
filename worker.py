#!/usr/bin/env python3
"""
Main entry point for py-analysis-worker
Run this file to start the worker
"""

import sys
import os

# Add the current directory to Python path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.main import main

if __name__ == '__main__':
    main()