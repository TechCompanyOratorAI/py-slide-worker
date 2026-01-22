"""
py-analysis-worker - Python worker for processing slides (OCR + embeddings)

A modular worker service that processes slide files uploaded to the OratorAI system.
Handles OCR text extraction, embedding generation, and webhook notifications.
"""

__version__ = "1.0.0"
__author__ = "OratorAI Team"

from .scripts.main import main, poll_queue

__all__ = ['main', 'poll_queue']