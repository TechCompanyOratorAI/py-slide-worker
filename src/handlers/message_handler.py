"""
Message handler module for py-analysis-worker
Handles SQS message processing and orchestrates slide processing workflow
"""

import json
import logging
from typing import Dict

from src.clients.aws_client import get_aws_client
from src.processors.slide_processor import get_slide_processor
from src.clients.webhook_client import get_webhook_client

logger = logging.getLogger(__name__)

class MessageHandler:
    """Handler for processing SQS messages"""
    
    def __init__(self):
        """Initialize message handler"""
        self.aws_client = get_aws_client()
        self.slide_processor = get_slide_processor()
        self.webhook_client = get_webhook_client()
        logger.info("Message handler initialized")
    
    def process_message(self, message: Dict, queue_url: str):
        """
        Process a single SQS message
        
        Args:
            message: SQS message dictionary
            queue_url: SQS queue URL for message deletion
        """
        try:
            body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            
            job_id = body.get('jobId')
            presentation_id = body.get('presentationId')
            slide_id = body.get('slideId')
            slide_url = body.get('slideUrl')
            slide_number = body.get('slideNumber')
            
            logger.info(f"📥 Processing message: jobId={job_id}, slideId={slide_id}, slideNumber={slide_number}")
            
            # Validate required fields
            if not all([job_id, presentation_id, slide_id, slide_url]):
                logger.error("Missing required fields in message")
                self._delete_message(queue_url, receipt_handle)
                return
            
            # Process slide
            result = self.slide_processor.process_slide(slide_url, slide_id)
            
            # Send webhook based on result
            if result['success']:
                webhook_sent = self.webhook_client.send_success_webhook(
                    job_id, presentation_id, slide_id, result
                )
            else:
                error_message = result.get('error', 'Processing failed')
                webhook_sent = self.webhook_client.send_failure_webhook(
                    job_id, presentation_id, slide_id, error_message
                )
            
            # Delete message from queue if webhook was sent successfully
            if webhook_sent:
                self._delete_message(queue_url, receipt_handle)
                logger.info(f"✅ Message processed and deleted for job {job_id}")
            else:
                logger.warning(f"⚠️ Webhook failed for job {job_id}, message not deleted (will retry)")
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}", exc_info=True)
            # Don't delete message on error - let it retry
    
    def _delete_message(self, queue_url: str, receipt_handle: str):
        """
        Delete message from SQS queue
        
        Args:
            queue_url: SQS queue URL
            receipt_handle: Message receipt handle
        """
        try:
            success = self.aws_client.delete_message(queue_url, receipt_handle)
            if not success:
                logger.warning("Failed to delete message from queue")
        except Exception as e:
            logger.error(f"Error deleting message: {e}")

# Global message handler instance
message_handler = None

def get_message_handler() -> MessageHandler:
    """Get singleton message handler instance"""
    global message_handler
    if message_handler is None:
        message_handler = MessageHandler()
    return message_handler