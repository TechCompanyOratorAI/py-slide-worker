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

# Add the parent directory (project root) to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import validate_config, SQS_QUEUE_URL
from config.memory_config import check_memory_usage, optimize_memory
from src.clients.aws_client import get_aws_client
from src.handlers.message_handler import get_message_handler

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
    Main loop: poll SQS queue for messages with health monitoring
    """
    global shutdown_requested
    
    logger.info(f"🚀 Starting slides worker, polling queue: {SQS_QUEUE_URL}")
    
    if not SQS_QUEUE_URL:
        logger.error("❌ AWS_SQS_SLIDES_QUEUE_URL not configured")
        sys.exit(1)
    
    # Initialize components
    aws_client = get_aws_client()
    message_handler = get_message_handler()
    
    # Health monitoring
    last_health_check = time.time()
    health_check_interval = 300  # 5 minutes
    message_count = 0
    
    logger.info("✅ Worker initialized successfully, starting message polling...")
    
    while not shutdown_requested:
        try:
            # Periodic health check
            current_time = time.time()
            if current_time - last_health_check > health_check_interval:
                logger.info(f"💓 Health check: processed {message_count} messages")
                check_memory_usage()
                optimize_memory()
                last_health_check = current_time
            
            # Long polling: wait up to 20 seconds for messages
            response = aws_client.receive_messages(
                queue_url=SQS_QUEUE_URL,
                max_messages=1,
                wait_time=20
            )
            
            messages = response.get('Messages', [])
            
            if messages:
                for message in messages:
                    if shutdown_requested:
                        logger.info("🛑 Shutdown requested, stopping message processing")
                        break
                    
                    try:
                        message_handler.process_message(message, SQS_QUEUE_URL)
                        message_count += 1
                    except Exception as e:
                        logger.error(f"❌ Error processing message: {e}", exc_info=True)
                        # Continue with next message
            else:
                logger.debug("No messages received, continuing to poll...")
                
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received, shutting down...")
            shutdown_requested = True
        except Exception as e:
            logger.error(f"❌ Error in poll loop: {e}", exc_info=True)
            # Continue polling even if there's an error, but wait a bit
            if not shutdown_requested:
                time.sleep(5)
    
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
