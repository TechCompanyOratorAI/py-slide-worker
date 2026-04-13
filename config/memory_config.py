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

# Memory limits for 2GB RAM instance
MAX_PDF_PAGES = 50  # Increased from 30 with more RAM headroom
MAX_IMAGE_SIZE = 20971520  # 20MB max image size (doubled from 10MB)
DPI_SETTING = 200  # DPI for OCR (200 is sufficient for slide content with large fonts;
                   # reduces memory per page ~55% vs 300 DPI)
MEMORY_LIMIT_MB = 1600  # 1600MB memory limit (80% of 2GB)
MEMORY_WARNING_MB = 1100  # Warning threshold at 1100MB (was 1200) — trigger GC earlier

# Intra-job page parallelism
# PAGE_WORKERS: concurrent pages/slides within ONE job
PAGE_WORKERS = int(os.getenv('PAGE_WORKERS', '2'))

# Global semaphore caps total concurrent OCR operations across ALL threads
# Budget: (MEMORY_LIMIT_MB - base_461MB) / ~250MB_per_ocr ≈ 4-5
# Keep at 4 to leave headroom for GC fragmentation
MAX_CONCURRENT_OCR = int(os.getenv('MAX_CONCURRENT_OCR', '4'))
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