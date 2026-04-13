"""
py-analysis-worker - Python worker for processing slides (OCR + embeddings)

This worker polls the AWS SQS slides queue and processes slide images:
1. Download slide from S3
2. Perform OCR to extract text
3. Generate embeddings for semantic search
4. Send results back via webhook

Entry point for the slide processing worker.
"""

import sys
import os
import logging
import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait

# Add the parent directory (project root) to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import validate_config, SQS_QUEUE_URL
from config.memory_config import (
    check_memory_usage, optimize_memory, is_memory_available, get_available_memory_mb
)
from src.clients.aws_client import get_aws_client
from src.handlers.message_handler import get_message_handler

# 1 job at a time: on 1 shared vCPU, parallelism hurts more than it helps.
# Tesseract gets the full CPU budget and finishes faster + more accurately.
WORKER_THREADS = int(os.getenv('WORKER_THREADS', '1'))

logger = logging.getLogger(__name__)

# Global shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


def poll_queue():
    """
    Main loop: poll SQS queue and process messages concurrently via ThreadPoolExecutor.

    Sliding-window approach:
    - Track pending futures; drain completed ones each iteration
    - Only fetch more messages when worker slots are available
    - Memory guard prevents fetching when RAM is low
    - Graceful shutdown waits for in-flight jobs to finish
    """
    global shutdown_requested

    logger.info(f"🚀 Starting slides worker (threads={WORKER_THREADS}), queue: {SQS_QUEUE_URL}")

    if not SQS_QUEUE_URL:
        logger.error("❌ AWS_SQS_SLIDES_QUEUE_URL not configured")
        sys.exit(1)

    aws_client = get_aws_client()
    message_handler = get_message_handler()

    last_health_check = time.time()
    health_check_interval = 300  # 5 minutes
    message_count = 0
    pending = set()

    logger.info("✅ Worker initialized, starting message polling...")

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        while not shutdown_requested:
            try:
                # --- Drain completed futures ---
                if pending:
                    done, pending = futures_wait(pending, timeout=0)
                    for f in done:
                        try:
                            f.result()
                            message_count += 1
                        except Exception as e:
                            logger.error(f"❌ Job failed: {e}", exc_info=True)

                # --- Periodic health check ---
                now = time.time()
                if now - last_health_check > health_check_interval:
                    logger.info(f"💓 Health: {message_count} done, {len(pending)} in-flight")
                    check_memory_usage()
                    optimize_memory()
                    last_health_check = now

                # --- Check capacity ---
                available = WORKER_THREADS - len(pending)
                if available <= 0:
                    time.sleep(0.1)
                    continue

                # --- Memory guard: 1 job at a time, generous headroom ---
                # With WORKER_THREADS=1 we only ever run 1 job concurrently.
                # Give it 600MB headroom so it never gets throttled.
                MEMORY_PER_JOB_MB = 600
                headroom_mb = get_available_memory_mb()
                affordable = min(available, max(0, int(headroom_mb // MEMORY_PER_JOB_MB)))
                if affordable == 0:
                    logger.warning(
                        f"⚠️ Low memory ({headroom_mb:.0f}MB headroom), "
                        "waiting before fetching more messages"
                    )
                    optimize_memory()
                    time.sleep(2)
                    continue
                available = affordable

                # --- Fetch messages (SQS max 10 per call) ---
                # Use shorter wait when workers are busy so we re-check capacity faster
                wait_time = 5 if pending else 20
                response = aws_client.receive_messages(
                    queue_url=SQS_QUEUE_URL,
                    max_messages=min(available, 10),
                    wait_time=wait_time
                )

                messages = response.get('Messages', [])
                if not messages:
                    logger.debug("No messages received, continuing to poll...")
                    continue

                for message in messages:
                    if shutdown_requested:
                        break
                    future = executor.submit(
                        message_handler.process_message, message, SQS_QUEUE_URL
                    )
                    pending.add(future)

                logger.debug(f"Submitted {len(messages)} jobs, {len(pending)} in-flight")

            except KeyboardInterrupt:
                logger.info("🛑 Keyboard interrupt, shutting down...")
                shutdown_requested = True
            except Exception as e:
                logger.error(f"❌ Poll loop error: {e}", exc_info=True)
                if not shutdown_requested:
                    time.sleep(5)

        # --- Graceful shutdown: wait for in-flight jobs ---
        if pending:
            logger.info(f"🛑 Shutdown requested, waiting for {len(pending)} in-flight jobs...")
            futures_wait(pending)

    logger.info(f"🏁 Worker shutdown complete. Processed {message_count} messages total.")


def main():
    """Main entry point with enhanced error handling"""
    try:
        # Setup signal handlers for graceful shutdown
        setup_signal_handlers()
        
        # Log startup information
        logger.info("🔧 Initializing py-analysis-worker...")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        # Validate configuration
        validate_config()
        logger.info("✅ Configuration validated")
        
        # Initial memory check
        check_memory_usage()
        logger.info("✅ Initial health check passed")
        
        # Start polling
        poll_queue()
        
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Failed to start worker: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🔄 Performing final cleanup...")
        optimize_memory()
        logger.info("👋 Worker shutdown complete")


if __name__ == '__main__':
    main()
