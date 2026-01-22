"""
Configuration package
"""

from .config import *

__all__ = [
    'AWS_REGION', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'SQS_QUEUE_URL',
    'WEBHOOK_URL', 'WEBHOOK_SECRET', 'OCR_LANGUAGE',
    'validate_config', 'check_library_availability'
]