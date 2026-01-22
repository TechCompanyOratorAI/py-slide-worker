"""
Processor modules for slide and OCR processing
"""

from .ocr_processor import get_ocr_processor, OCRProcessor
from .slide_processor import get_slide_processor, SlideProcessor

__all__ = ['get_ocr_processor', 'OCRProcessor', 'get_slide_processor', 'SlideProcessor']