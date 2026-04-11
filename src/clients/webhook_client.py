"""
Webhook client module for py-analysis-worker
Handles sending processing results back to Node API via webhooks
"""

import logging
import requests
from typing import Dict, Optional, Tuple
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
    
    def send_success_webhook(
        self, job_id: int, presentation_id: int, slide_id: int, result: Dict
    ) -> Tuple[bool, bool]:
        """
        Send success webhook with processing results.

        Returns:
            (success, should_retry) tuple
        """
        payload = {
            'jobId': job_id,
            'presentationId': presentation_id,
            'slideId': slide_id,
            'status': 'success',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'result': {
                'extractedText': result.get('extractedText'),
                'pages': result.get('pages'),
                'embedding': result.get('embedding')
            }
        }
        return self._send_webhook(payload, job_id)

    def send_failure_webhook(
        self, job_id: int, presentation_id: int, slide_id: int, error: str
    ) -> Tuple[bool, bool]:
        """
        Send failure webhook with error message.

        Returns:
            (success, should_retry) tuple
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

    def _send_webhook(self, payload: Dict, job_id: int) -> Tuple[bool, bool]:
        """
        Send webhook request.

        Returns:
            (success, should_retry)
            - (True, False)  on HTTP 2xx
            - (False, False) on HTTP 4xx  – permanent failure, don't retry
            - (False, True)  on HTTP 5xx or network error – transient, retry later
        """
        headers = {'Content-Type': 'application/json'}
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
            return (True, False)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            if 400 <= status_code < 500:
                logger.warning(
                    f"⚠️ Webhook permanent failure for job {job_id} "
                    f"(HTTP {status_code}) – job likely deleted, will discard message"
                )
                return (False, False)
            logger.error(f"❌ Failed to send webhook for job {job_id}: {e}")
            return (False, True)
        except Exception as e:
            logger.error(f"❌ Failed to send webhook for job {job_id}: {e}")
            return (False, True)

# Global webhook client instance
webhook_client = None

def get_webhook_client() -> WebhookClient:
    """Get singleton webhook client instance"""
    global webhook_client
    if webhook_client is None:
        webhook_client = WebhookClient()
    return webhook_client