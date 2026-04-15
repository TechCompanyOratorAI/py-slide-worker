"""
Configuration module for py-analysis-worker
Handles environment variables and application settings
"""

import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'ap-southeast-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
SQS_QUEUE_URL = os.getenv('AWS_SQS_SLIDES_QUEUE_URL')

# Webhook Configuration
WEBHOOK_URL = os.getenv('WEBHOOK_URL', 'http://localhost:8080/api/v1/webhooks/slides-complete')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')

# OCR Configuration
OCR_LANGUAGE = os.getenv('OCR_LANGUAGE', 'vi,en')

# Check required environment variables
def validate_config():
    """Validate that required environment variables are set"""
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY', 
        'AWS_SQS_SLIDES_QUEUE_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return True

# Library availability flags
def check_library_availability():
    """Check which optional libraries are available"""
    availability = {
        'OCR_AVAILABLE': False,
        'PDF2IMAGE_AVAILABLE': False,
        'CV2_AVAILABLE': False,
        'EASYOCR_AVAILABLE': False,
        'PYMUPDF_AVAILABLE': False,
        'PDFPLUMBER_AVAILABLE': False,
        'PPTX_AVAILABLE': False,
    }
    
    logger = logging.getLogger(__name__)
    
    # Check OCR libraries
    try:
        import easyocr
        from PIL import Image, ImageEnhance, ImageFilter
        availability['OCR_AVAILABLE'] = True
        availability['EASYOCR_AVAILABLE'] = True
    except ImportError:
        logger.warning("⚠️ easyocr or Pillow not installed. OCR will not work.")
    
    try:
        from pdf2image import convert_from_path, convert_from_bytes
        availability['PDF2IMAGE_AVAILABLE'] = True
    except ImportError:
        logger.warning("⚠️ pdf2image not installed. PDF processing will be limited.")
    
    try:
        import cv2
        import numpy as np
        availability['CV2_AVAILABLE'] = True
    except ImportError:
        logger.warning("⚠️ opencv-python not installed. Image enhancement will be limited.")
    
    try:
        import fitz  # PyMuPDF
        availability['PYMUPDF_AVAILABLE'] = True
    except ImportError:
        logger.info("ℹ️ PyMuPDF not installed. Text-first PDF extraction will be limited.")
    
    try:
        import pdfplumber
        availability['PDFPLUMBER_AVAILABLE'] = True
    except ImportError:
        logger.info("ℹ️ pdfplumber not installed. Advanced PDF text extraction will be limited.")

    try:
        from pptx import Presentation
        availability['PPTX_AVAILABLE'] = True
    except ImportError:
        logger.warning("⚠️ python-pptx not installed. PPTX file processing will not work.")

    return availability