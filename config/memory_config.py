"""
Memory optimization configuration for py-analysis-worker
"""
import os
import gc
import logging

logger = logging.getLogger(__name__)

# Memory limits for 1GB RAM instance
MAX_PDF_PAGES = int(os.getenv('MAX_PDF_PAGES', '30'))  # More pages allowed
MAX_IMAGE_SIZE = int(os.getenv('MAX_IMAGE_SIZE', '20971520'))  # 20MB max image
DPI_SETTING = int(os.getenv('OCR_DPI', '250'))  # Higher DPI for better quality

def optimize_memory():
    """Force garbage collection to free memory"""
    try:
        collected = gc.collect()
        logger.debug(f"Garbage collected {collected} objects")
    except Exception as e:
        logger.warning(f"Memory optimization failed: {e}")

def check_memory_usage():
    """Check current memory usage (if psutil available)"""
    try:
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"Current memory usage: {memory_mb:.1f} MB")
        
        # Warning if memory > 800MB (80% of 1GB)
        if memory_mb > 800:
            logger.warning(f"⚠️ High memory usage: {memory_mb:.1f} MB")
            optimize_memory()
            
        return memory_mb
    except ImportError:
        logger.debug("psutil not available for memory monitoring")
        return None
    except Exception as e:
        logger.warning(f"Memory check failed: {e}")
        return None