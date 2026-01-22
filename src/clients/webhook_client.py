"""
Webhook client module for py-analysis-worker
Handles sending processing results back to Node API via webhooks
"""

import logging
import requests
from typing import Dict, Optional
from datetime import datetime, timezone

from config.config import WEBHOOK_URL, WEBHOOK_SECRET

logger = logging.getLogger(__name__)

class WebhookClient:
    """Client for sending webhook notifications"""
    
    def __init__(self):
        """Initialize webhook client"""
        self.webhook_url = WEBHOOK_URL
        self.webhook_secret = WEBHOOK_SECRET
        logger.info(f"Webhook client initialized with URL: {self.webhook_url}")
    
    def send_success_webhook(self, job_id: int, presentation_id: int, slide_id: int, result: Dict) -> bool:
        """
        Send success webhook with processing results
        
        Args:
            job_id: Job ID
            presentation_id: Presentation ID
            slide_id: Slide ID
            result: Processing results dictionary
            
        Returns:
            True if successful, False otherwise
        """
        payload = {
            'jobId': job_id,
            'presentationId': presentation_id,
            'slideId': slide_id,
            'status': 'success',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'result': {
                'extractedText': result.get('extractedText'),  # Combined text from all pages
                'pages': result.get('pages'),  # List of {pageNumber, text} for multi-page files
                'embedding': result.get('embedding')
            }
        }
        
        return self._send_webhook(payload, job_id)
    
    def send_failure_webhook(self, job_id: int, presentation_id: int, slide_id: int, error: str) -> bool:
        """
        Send failure webhook with error message
        
        Args:
            job_id: Job ID
            presentation_id: Presentation ID
            slide_id: Slide ID
            error: Error message
            
        Returns:
            True if successful, False otherwise
        """
        payload = {
            'jobId': job_id,
            'presentationId': presentation_id,
            'slideId': slide_id,
            'status': 'failed',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': error
        }
        
        return self._send_webhook(payload, job_id)
    
    def _send_webhook(self, payload: Dict, job_id: int) -> bool:
        """
        Send webhook request
        
        Args:
            payload: Webhook payload
            job_id: Job ID for logging
            
        Returns:
            True if successful, False otherwise
        """
        headers = {
            'Content-Type': 'application/json'
        }
        
        if self.webhook_secret:
            headers['Authorization'] = f'Bearer {self.webhook_secret}'
        
        try:
            response = requests.post(
                self.webhook_url, 
                json=payload, 
                headers=headers, 
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"✅ Webhook sent successfully for job {job_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send webhook for job {job_id}: {e}")
            return False

# Global webhook client instance
webhook_client = None

def get_webhook_client() -> WebhookClient:
    """Get singleton webhook client instance"""
    global webhook_client
    if webhook_client is None:
        webhook_client = WebhookClient()
    return webhook_client