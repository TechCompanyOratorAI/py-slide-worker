"""
Memory optimization configuration for py-analysis-worker
"""
import os
import gc
import logging
import threading

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Memory limits for 16GB RAM instance
MAX_PDF_PAGES = 50
MAX_IMAGE_SIZE = 20971520  # 20MB max image size
DPI_SETTING = 300  # DPI for OCR
MEMORY_LIMIT_MB = 13312  # 13GB limit (80% of 16GB)
MEMORY_WARNING_MB = 10240  # Warning threshold at 10GB

# Intra-job page parallelism
# PAGE_WORKERS=1: sequential page processing — optimal for 1 shared vCPU.
# Multiple threads on 1 vCPU cause context switching that slows Tesseract.
PAGE_WORKERS = int(os.getenv('PAGE_WORKERS', '1'))

# Only 1 OCR at a time (matches PAGE_WORKERS=1 + WORKER_THREADS=1)
MAX_CONCURRENT_OCR = int(os.getenv('MAX_CONCURRENT_OCR', '1'))
_ocr_semaphore = threading.Semaphore(MAX_CONCURRENT_OCR)

def optimize_memory():
    """Force garbage collection to free memory"""
    try:
        collected = gc.collect()
        logger.debug(f"Garbage collected {collected} objects")
    except Exception as e:
        logger.warning(f"Memory optimization failed: {e}")

def check_memory_usage():
    """Check current memory usage and enforce limits"""
    if not PSUTIL_AVAILABLE:
        logger.debug("psutil not available for memory monitoring")
        return None
        
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        logger.info(f"Current memory usage: {memory_mb:.1f} MB")
        
        # Critical memory limit - force cleanup and potentially exit
        if memory_mb > MEMORY_LIMIT_MB:
            logger.error(f"🚨 CRITICAL: Memory usage {memory_mb:.1f} MB exceeds limit {MEMORY_LIMIT_MB} MB")
            optimize_memory()
            # Check again after cleanup
            memory_mb = process.memory_info().rss / 1024 / 1024
            if memory_mb > MEMORY_LIMIT_MB:
                logger.error("🚨 Memory still high after cleanup - potential OOM risk")
                raise MemoryError(f"Memory usage {memory_mb:.1f} MB exceeds safe limit")
        
        # Warning threshold
        elif memory_mb > MEMORY_WARNING_MB:
            logger.warning(f"⚠️ High memory usage: {memory_mb:.1f} MB")
            optimize_memory()
            
        return memory_mb
    except Exception as e:
        logger.warning(f"Memory check failed: {e}")
        return None

def get_available_memory_mb() -> float:
    """
    Return remaining headroom (MB) before MEMORY_LIMIT_MB is hit.
    Returns float('inf') if psutil is unavailable.
    """
    if not PSUTIL_AVAILABLE:
        return float('inf')
    try:
        process = psutil.Process()
        current_mb = process.memory_info().rss / 1024 / 1024
        return max(0.0, MEMORY_LIMIT_MB - current_mb)
    except:
        return float('inf')

def is_memory_available(required_mb: int = 100) -> bool:
    """
    Check if enough memory is available for an operation
    
    Args:
        required_mb: Required memory in MB
        
    Returns:
        True if enough memory available
    """
    available_mb = get_available_memory_mb()
    if available_mb < required_mb:
        logger.warning(f"⚠️ Insufficient memory: need {required_mb}MB, available {available_mb:.1f}MB")
        return False
    return True