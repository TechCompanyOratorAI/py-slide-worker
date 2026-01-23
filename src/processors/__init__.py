"""
Processor modules for slide and OCR processing
"""

from .ocr_processor import get_ocr_processor, OCRProcessor
from .slide_processor import get_slide_processor, SlideProcessor
from .pdf_text_extractor import get_pdf_text_extractor, PDFTextExtractor

__all__ = ['get_ocr_processor', 'OCRProcessor', 'get_slide_processor', 'SlideProcessor', 'get_pdf_text_extractor', 'PDFTextExtractor']