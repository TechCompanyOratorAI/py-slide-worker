"""
AWS client module for py-analysis-worker
Handles SQS and S3 client initialization and operations
"""

import boto3
import logging
from typing import Dict, Optional
from config.config import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

logger = logging.getLogger(__name__)

class AWSClient:
    """AWS client wrapper for SQS and S3 operations"""
    
    def __init__(self):
        """Initialize AWS clients"""
        self.sqs_client = boto3.client(
            'sqs',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        self.s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        logger.info("AWS clients initialized")
    
    def receive_messages(self, queue_url: str, max_messages: int = 1, wait_time: int = 20) -> Dict:
        """
        Receive messages from SQS queue
        
        Args:
            queue_url: SQS queue URL
            max_messages: Maximum number of messages to receive
            wait_time: Long polling wait time in seconds
            
        Returns:
            SQS response dictionary
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=['All']
            )
            return response
        except Exception as e:
            logger.error(f"Failed to receive messages from SQS: {e}")
            raise
    
    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Delete message from SQS queue
        
        Args:
            queue_url: SQS queue URL
            receipt_handle: Message receipt handle
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete message from SQS: {e}")
            return False
    
    def download_from_s3(self, bucket: str, key: str, local_path: str) -> str:
        """
        Download file from S3.

        Returns:
            'ok'         – download succeeded
            'not_found'  – S3 object does not exist (404 / NoSuchKey)
            'error'      – any other failure
        """
        try:
            # HeadObject is already called by boto3's download_file internally,
            # but we catch the ClientError code explicitly here for clarity.
            self.s3_client.download_file(bucket, key, local_path)
            logger.info(f"Downloaded from S3: s3://{bucket}/{key} -> {local_path}")
            return 'ok'
        except Exception as e:
            error_code = getattr(getattr(e, 'response', None), 'get', lambda *a: None)('Error', {}).get('Code', '')
            # boto3 ClientError exposes the code via e.response['Error']['Code']
            try:
                error_code = e.response['Error']['Code']  # type: ignore[attr-defined]
            except (AttributeError, KeyError, TypeError):
                error_code = str(e)
            if error_code in ('404', 'NoSuchKey'):
                logger.warning(f"S3 object not found (404): s3://{bucket}/{key}")
                return 'not_found'
            logger.error(f"Failed to download from S3: {e}")
            return 'error'

# Global AWS client instance
aws_client = None

def get_aws_client() -> AWSClient:
    """Get singleton AWS client instance"""
    global aws_client
    if aws_client is None:
        aws_client = AWSClient()
    return aws_client