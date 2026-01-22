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

# Add the parent directory (project root) to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import validate_config, SQS_QUEUE_URL
from src.clients.aws_client import get_aws_client
from src.handlers.message_handler import get_message_handler

logger = logging.getLogger(__name__)


def poll_queue():
    """
    Main loop: poll SQS queue for messages
    """
    logger.info(f"🚀 Starting slides worker, polling queue: {SQS_QUEUE_URL}")
    
    if not SQS_QUEUE_URL:
        logger.error("❌ AWS_SQS_SLIDES_QUEUE_URL not configured")
        sys.exit(1)
    
    # Initialize components
    aws_client = get_aws_client()
    message_handler = get_message_handler()
    
    while True:
        try:
            # Long polling: wait up to 20 seconds for messages
            response = aws_client.receive_messages(
                queue_url=SQS_QUEUE_URL,
                max_messages=1,
                wait_time=20
            )
            
            messages = response.get('Messages', [])
            
            if messages:
                for message in messages:
                    message_handler.process_message(message, SQS_QUEUE_URL)
            else:
                logger.debug("No messages received, continuing to poll...")
                
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down worker...")
            break
        except Exception as e:
            logger.error(f"❌ Error in poll loop: {e}", exc_info=True)
            # Continue polling even if there's an error
            time.sleep(5)


def main():
    """Main entry point"""
    try:
        # Validate configuration
        validate_config()
        logger.info("✅ Configuration validated")
        
        # Start polling
        poll_queue()
        
    except Exception as e:
        logger.error(f"❌ Failed to start worker: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
