"""
PDF text extraction module - Text-first approach with OCR fallback
Optimized for slide alignment and memory efficiency
"""

import os
import logging
from typing import List, Dict, Optional, Tuple
from config.config import check_library_availability
from config.memory_config import check_memory_usage, is_memory_available, optimize_memory, DPI_SETTING

logger = logging.getLogger(__name__)

# Check library availability
LIBS = check_library_availability()

# Import libraries if available
if LIBS.get('PYMUPDF_AVAILABLE', False):
    import fitz  # PyMuPDF
    
if LIBS.get('PDFPLUMBER_AVAILABLE', False):
    import pdfplumber

class PDFTextExtractor:
    """
    Smart PDF text extraction with text-first approach
    Falls back to OCR only when necessary
    """
    
    def __init__(self, ocr_processor=None):
        """Initialize PDF text extractor"""
        self.ocr_processor = ocr_processor
        self.min_text_threshold = 20  # Minimum chars to consider page has text
        logger.info("PDF text extractor initialized")
    
    def extract_text_pymupdf(self, pdf_path: str) -> List[Dict]:
        """
        Extract text using PyMuPDF (fastest method)
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of {pageNumber, text, hasText, needsOCR}
        """
        if not LIBS.get('PYMUPDF_AVAILABLE', False):
            logger.warning("PyMuPDF not available")
            return []
        
        try:
            pages_data = []
            doc = fitz.open(pdf_path)
            
            logger.info(f"Extracting text from {len(doc)} pages using PyMuPDF")
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text().strip()
                
                # Determine if page needs OCR
                has_text = len(text) >= self.min_text_threshold
                needs_ocr = not has_text
                
                pages_data.append({
                    'pageNumber': page_num + 1,
                    'text': text if has_text else '',
                    'hasText': has_text,
                    'needsOCR': needs_ocr,
                    'extractionMethod': 'pymupdf'
                })
                
                logger.debug(f"Page {page_num + 1}: {len(text)} chars, needs_ocr={needs_ocr}")
            
            doc.close()
            return pages_data
            
        except Exception as e:
            logger.error(f"PyMuPDF extraction failed: {e}")
            return []
    
    def extract_text_pdfplumber(self, pdf_path: str) -> List[Dict]:
        """
        Extract text using pdfplumber (good for tables/structured content)
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of {pageNumber, text, hasText, needsOCR}
        """
        if not LIBS.get('PDFPLUMBER_AVAILABLE', False):
            logger.warning("pdfplumber not available")
            return []
        
        try:
            pages_data = []
            
            with pdfplumber.open(pdf_path) as pdf:
                logger.info(f"Extracting text from {len(pdf.pages)} pages using pdfplumber")
                
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text() or ''
                    text = text.strip()
                    
                    # Determine if page needs OCR
                    has_text = len(text) >= self.min_text_threshold
                    needs_ocr = not has_text
                    
                    pages_data.append({
                        'pageNumber': page_num + 1,
                        'text': text if has_text else '',
                        'hasText': has_text,
                        'needsOCR': needs_ocr,
                        'extractionMethod': 'pdfplumber'
                    })
                    
                    logger.debug(f"Page {page_num + 1}: {len(text)} chars, needs_ocr={needs_ocr}")
            
            return pages_data
            
        except Exception as e:
            logger.error(f"pdfplumber extraction failed: {e}")
            return []
    
    def smart_extract_with_ocr_fallback(self, pdf_path: str) -> List[Dict]:
        """
        Smart extraction: text-first, OCR fallback for image-only pages
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of {pageNumber, text, extractionMethod, confidence}
        """
        if not is_memory_available(200):
            logger.error("Insufficient memory for PDF text extraction")
            return []
        
        # Step 1: Try text extraction first (PyMuPDF preferred)
        pages_data = self.extract_text_pymupdf(pdf_path)
        if not pages_data:
            pages_data = self.extract_text_pdfplumber(pdf_path)
        
        if not pages_data:
            logger.error("All text extraction methods failed")
            return []
        
        # Step 2: OCR fallback for pages that need it
        ocr_pages = [p for p in pages_data if p['needsOCR']]
        text_pages = [p for p in pages_data if not p['needsOCR']]
        
        logger.info(f"Text extraction results: {len(text_pages)} pages with text, {len(ocr_pages)} need OCR")
        
        # OCR fallback for image-only pages
        if ocr_pages and self.ocr_processor:
            logger.info(f"Running OCR fallback for {len(ocr_pages)} pages")
            ocr_results = self._ocr_fallback_pages(pdf_path, ocr_pages)
            
            # Merge OCR results back
            for ocr_result in ocr_results:
                for page_data in pages_data:
                    if page_data['pageNumber'] == ocr_result['pageNumber']:
                        page_data['text'] = ocr_result['text']
                        page_data['extractionMethod'] = 'ocr_fallback'
                        page_data['hasText'] = bool(ocr_result['text'].strip())
                        break
        
        # Final cleanup and formatting
        final_results = []
        for page_data in pages_data:
            final_results.append({
                'pageNumber': page_data['pageNumber'],
                'text': page_data['text'].strip(),
                'extractionMethod': page_data['extractionMethod'],
                'confidence': 'high' if page_data['hasText'] else 'low'
            })
        
        # Memory cleanup
        optimize_memory()
        
        return final_results
    
    def _ocr_fallback_pages(self, pdf_path: str, ocr_pages: List[Dict]) -> List[Dict]:
        """
        Run OCR only on specific pages that need it
        
        Args:
            pdf_path: Path to PDF file
            ocr_pages: List of pages that need OCR
            
        Returns:
            List of OCR results for those pages
        """
        if not self.ocr_processor:
            logger.warning("No OCR processor available for fallback")
            return []
        
        try:
            from pdf2image import convert_from_path

            page_numbers = [p['pageNumber'] for p in ocr_pages]
            logger.info(f"OCR fallback: {len(page_numbers)} pages (sequential, 1 vCPU mode)")

            results: Dict[int, str] = {}
            for page_num in page_numbers:
                temp_img_path = None
                try:
                    images = convert_from_path(
                        pdf_path,
                        dpi=DPI_SETTING,
                        fmt='png',
                        first_page=page_num,
                        last_page=page_num
                    )
                    if not images:
                        logger.warning(f"No image generated for page {page_num}")
                        results[page_num] = ''
                        continue

                    temp_img_path = pdf_path.replace('.pdf', f'_ocr_page_{page_num}.png')
                    images[0].save(temp_img_path, 'PNG', quality=85, optimize=True)
                    del images
                    optimize_memory()

                    text = self.ocr_processor.extract_text_from_image(temp_img_path)
                    char_count = len(text or '')
                    logger.info(f"OCR page {page_num}: {char_count} chars")
                    results[page_num] = text or ''

                except Exception as e:
                    logger.error(f"OCR page {page_num} failed: {e}")
                    results[page_num] = ''
                finally:
                    if temp_img_path:
                        try:
                            if os.path.exists(temp_img_path):
                                os.remove(temp_img_path)
                            base, ext = os.path.splitext(temp_img_path)
                            enhanced = f"{base}_enhanced{ext}"
                            if os.path.exists(enhanced):
                                os.remove(enhanced)
                        except Exception:
                            pass

            # Return in original page order — critical for correct PDF structure
            return [{'pageNumber': pn, 'text': results.get(pn, '')} for pn in page_numbers]

        except Exception as e:
            logger.error(f"OCR fallback failed: {e}")
            return []

# Global PDF text extractor instance + lock for thread-safe initialisation
import threading as _threading
_pdf_extractor_lock = _threading.Lock()
_pdf_text_extractor: "PDFTextExtractor | None" = None

def get_pdf_text_extractor(ocr_processor=None) -> "PDFTextExtractor":
    """
    Get the singleton PDFTextExtractor instance.

    Thread-safe: uses a module-level lock so that concurrent calls from
    multiple SQS-worker threads cannot each create their own instance.

    If the singleton was previously created with ocr_processor=None but a
    real processor is now available, the processor is wired in retroactively
    so OCR fallback becomes available without restarting the process.
    """
    global _pdf_text_extractor
    with _pdf_extractor_lock:
        if _pdf_text_extractor is None:
            _pdf_text_extractor = PDFTextExtractor(ocr_processor)
            logger.info("PDFTextExtractor singleton created")
        elif ocr_processor is not None and _pdf_text_extractor.ocr_processor is None:
            # Retroactively attach a real OCR processor that wasn't available
            # when the singleton was first constructed.
            _pdf_text_extractor.ocr_processor = ocr_processor
            logger.info("PDFTextExtractor: ocr_processor updated (was None)")
    return _pdf_text_extractor