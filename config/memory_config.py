"""
Memory optimization configuration for py-analysis-worker
"""
import os
import gc
import logging

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Memory limits for 1GB RAM instance (hardcoded for stability)
MAX_PDF_PAGES = 30  # Fixed at 30 pages as requested
MAX_IMAGE_SIZE = 10485760  # 10MB max image size
DPI_SETTING = 150  # Fixed DPI for OCR processing
MEMORY_LIMIT_MB = 800  # 800MB memory limit (80% of 1GB)
MEMORY_WARNING_MB = 600  # Warning threshold at 600MB

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

def is_memory_available(required_mb: int = 100) -> bool:
    """
    Check if enough memory is available for an operation
    
    Args:
        required_mb: Required memory in MB
        
    Returns:
        True if enough memory available
    """
    if not PSUTIL_AVAILABLE:
        return True  # Assume OK if can't check
        
    try:
        process = psutil.Process()
        current_mb = process.memory_info().rss / 1024 / 1024
        available_mb = MEMORY_LIMIT_MB - current_mb
        
        if available_mb < required_mb:
            logger.warning(f"⚠️ Insufficient memory: need {required_mb}MB, available {available_mb:.1f}MB")
            return False
        return True
    except:
        return True  # Assume OK if can't check