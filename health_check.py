#!/usr/bin/env python3
"""
Health check script for py-analysis-worker
Returns exit code 0 if healthy, 1 if unhealthy
"""

import sys
import os
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from config.memory_config import check_memory_usage, PSUTIL_AVAILABLE
    from config.config import validate_config
    
    def health_check():
        """Perform basic health checks"""
        try:
            # Check configuration
            validate_config()
            
            # Check memory if available
            if PSUTIL_AVAILABLE:
                memory_mb = check_memory_usage()
                if memory_mb and memory_mb > 900:  # 900MB is critical
                    print(f"UNHEALTHY: Memory usage too high: {memory_mb:.1f} MB")
                    return False
            
            print("HEALTHY: All checks passed")
            return True
            
        except Exception as e:
            print(f"UNHEALTHY: Health check failed: {e}")
            return False
    
    if __name__ == '__main__':
        # Suppress logs for health check
        logging.disable(logging.CRITICAL)
        
        if health_check():
            sys.exit(0)
        else:
            sys.exit(1)
            
except Exception as e:
    print(f"UNHEALTHY: Failed to import modules: {e}")
    sys.exit(1)