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
    
    def download_from_s3(self, bucket: str, key: str, local_path: str) -> bool:
        """
        Download file from S3
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local file path to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.download_file(bucket, key, local_path)
            logger.info(f"Downloaded from S3: s3://{bucket}/{key} -> {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download from S3: {e}")
            return False

# Global AWS client instance
aws_client = None

def get_aws_client() -> AWSClient:
    """Get singleton AWS client instance"""
    global aws_client
    if aws_client is None:
        aws_client = AWSClient()
    return aws_client