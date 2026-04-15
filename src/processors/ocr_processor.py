"""
OCR processing module for py-analysis-worker
Handles text extraction from images and PDFs using easyocr
"""

import os
import logging
import threading
from typing import Optional, List, Dict
from config.config import OCR_LANGUAGE, check_library_availability
from config.memory_config import check_memory_usage, is_memory_available, optimize_memory, MAX_IMAGE_SIZE, _ocr_semaphore

logger = logging.getLogger(__name__)

# Check library availability
LIBS = check_library_availability()

# Import libraries if available
if LIBS['OCR_AVAILABLE']:
    from PIL import Image
    if LIBS.get('EASYOCR_AVAILABLE'):
        import easyocr

if LIBS['PDF2IMAGE_AVAILABLE']:
    from pdf2image import convert_from_path, convert_from_bytes

if LIBS['CV2_AVAILABLE']:
    import cv2
    import numpy as np

class OCRProcessor:
    """OCR processor for extracting text from images and PDFs using EasyOCR"""
    
    def __init__(self):
        """Initialize OCR processor"""
        self.easyocr_reader = None
        
        if LIBS.get('EASYOCR_AVAILABLE'):
            try:
                # Handle old Tesseract format from .env (e.g. 'vie+eng')
                # Translate 'vie' -> 'vi' and 'eng' -> 'en'
                langs_str = OCR_LANGUAGE.replace('+', ',').replace('vie', 'vi').replace('eng', 'en')
                
                langs = [lang.strip() for lang in langs_str.split(',') if lang.strip()]
                if not langs:
                    langs = ['vi', 'en']
                    
                # Initialize with languages and gpu=False to save RAM on standard instances
                logger.info(f"Loading EasyOCR model for languages: {langs}...")
                self.easyocr_reader = easyocr.Reader(langs, gpu=False)
                logger.info("✅ EasyOCR initialized successfully")
            except Exception as e:
                logger.error(f"⚠️ Failed to initialize EasyOCR: {e}")
                self.easyocr_reader = None
        else:
            logger.error("EasyOCR library is not available.")
            
        logger.info("OCR processor initialized")

    def enhance_image(self, image_path: str) -> Optional[str]:
        """
        Enhance image quality for better OCR results.
        Uses bilateral filtering which works well for EasyOCR.
        
        Args:
            image_path: Path to original image
            
        Returns:
            Path to enhanced image or original if failed
        """
        if not LIBS['CV2_AVAILABLE']:
            return image_path
        
        try:
            img = cv2.imread(image_path)
            if img is None:
                return image_path
            
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

            # Invert dark-background slides for better thresholding
            if gray.mean() < 128:
                gray = cv2.bitwise_not(gray)

            # Apply bilateral filter to remove noise while keeping edges sharp
            denoised = cv2.bilateralFilter(gray, 9, 75, 75)

            # Contrast enhancement
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(denoised)

            # Binarization
            _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

            base, ext = os.path.splitext(image_path)
            enhanced_path = f"{base}_enhanced{ext}"
            cv2.imwrite(enhanced_path, binary)
            
            return enhanced_path
            
        except Exception as e:
            logger.warning(f"Image enhancement failed: {e}. Using original image.")
            return image_path
    
    def extract_text_from_image(self, image_path: str) -> Optional[str]:
        """
        Extract text from a single image using EasyOCR.

        Args:
            image_path: Path to image file.

        Returns:
            Extracted text string, or None if failed.
        """
        if not self.easyocr_reader:
            logger.error("EasyOCR reader is not initialized.")
            return None

        # Check file size
        try:
            file_size = os.path.getsize(image_path)
            if file_size > MAX_IMAGE_SIZE:
                logger.error(f"Image too large: {file_size} bytes limit")
                return None
        except Exception as e:
            logger.warning(f"Could not check file size: {e}")

        # Check memory (Require 300MB headroom for EasyOCR processing over loaded model)
        if not is_memory_available(300):
            logger.error("Insufficient memory for EasyOCR processing")
            return None

        with _ocr_semaphore:
            try:
                check_memory_usage()
                logger.info(f"[EASYOCR] Processing image: {image_path}")
                
                enhanced_path = self.enhance_image(image_path)
                target_img = enhanced_path if enhanced_path != image_path else image_path
                
                # Diagnostic log before OCR processing
                try:
                    import psutil
                    mem_before = psutil.Process().memory_info().rss / 1024 / 1024
                    logger.info(f"[DIAGNOSTIC] RAM before decode: {mem_before:.1f} MB")
                except ImportError:
                    mem_before = 0
                
                results = self.easyocr_reader.readtext(target_img)

                # Diagnostic log after OCR processing
                try:
                    import psutil
                    mem_after = psutil.Process().memory_info().rss / 1024 / 1024
                    delta = mem_after - mem_before if mem_before > 0 else 0
                    logger.info(f"[DIAGNOSTIC] RAM after decode: {mem_after:.1f} MB (Delta: {delta:+.1f} MB)")
                except ImportError:
                    pass

                text_parts = []
                for (bbox, text, confidence) in results:
                    if confidence > 0.4:  # Adjust threshold if needed
                        text_parts.append(text)

                combined_text = '\n'.join(text_parts).strip()

                if combined_text:
                    logger.info(f"[EASYOCR] Extracted {len(combined_text)} chars")
                    return combined_text
                    
                logger.warning(f"[EASYOCR] No text extracted from '{image_path}'")
                return None

            except MemoryError as e:
                logger.error(f"OCR failed due to memory exhaustion: {e}")
                optimize_memory()
                return None
            except Exception as e:
                logger.error(f"OCR failed for {image_path}: {e}", exc_info=True)
                optimize_memory()
                return None
            finally:
                optimize_memory()
    
    def process_pdf_to_pages(self, pdf_path: str) -> List[Dict]:
        """
        Process PDF file: convert each page to image and extract text with memory management
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of dictionaries with pageNumber and text
        """
        if not LIBS['PDF2IMAGE_AVAILABLE'] or not LIBS['OCR_AVAILABLE']:
            logger.error("PDF processing libraries not available")
            return []
        
        # Check memory before starting
        if not is_memory_available(300):  # Need ~300MB for PDF processing
            logger.error("Insufficient memory for PDF processing")
            return []
        
        try:
            pages_data = []
            
            # Convert PDF to images with memory-optimized settings
            logger.info(f"Converting PDF to images: {pdf_path}")
            from config.memory_config import DPI_SETTING, MAX_PDF_PAGES
            
            # Use memory-optimized DPI
            images = convert_from_path(pdf_path, dpi=DPI_SETTING, fmt='png', 
                                    first_page=1, last_page=MAX_PDF_PAGES)
            
            logger.info(f"PDF has {len(images)} pages")
            
            # Process each page with memory monitoring
            for page_num, image in enumerate(images, start=1):
                try:
                    # Check memory before processing each page
                    if not is_memory_available(100):
                        logger.warning(f"Stopping PDF processing at page {page_num} due to low memory")
                        break
                    
                    # Save temporary image
                    temp_image_path = pdf_path.replace('.pdf', f'_page_{page_num}.png')
                    image.save(temp_image_path, 'PNG', quality=85, optimize=True)
                    
                    # Clear the image from memory immediately
                    del image
                    
                    # Force aggressive memory cleanup
                    import gc
                    gc.collect()
                    optimize_memory()
                    
                    # Extract text using OCR
                    text = self.extract_text_from_image(temp_image_path)
                    
                    if text:
                        pages_data.append({
                            'pageNumber': page_num,
                            'text': text
                        })
                        logger.info(f"Page {page_num}: Extracted {len(text)} characters")
                    else:
                        logger.warning(f"Page {page_num}: No text extracted")
                    
                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {e}")
                    
                finally:
                    # Cleanup temp image and enhanced image if exists
                    try:
                        if 'temp_image_path' in locals():
                            if os.path.exists(temp_image_path):
                                os.remove(temp_image_path)
                            enhanced_path = temp_image_path.replace('.png', '_enhanced.png')
                            if os.path.exists(enhanced_path):
                                os.remove(enhanced_path)
                    except Exception as e:
                        logger.debug(f"Failed to cleanup temp files: {e}")
                    
                    # Force memory cleanup after each page
                    optimize_memory()
            
            return pages_data
            
        except MemoryError as e:
            logger.error(f"PDF processing failed due to memory exhaustion: {e}")
            optimize_memory()
            return pages_data  # Return partial results
        except Exception as e:
            logger.error(f"PDF processing failed: {e}", exc_info=True)
            return pages_data  # Return partial results if any
        finally:
            # Final memory cleanup
            optimize_memory()
    
    def cleanup_enhanced_image(self, original_path: str):
        """Clean up enhanced image file if it exists"""
        try:
            base, ext = os.path.splitext(original_path)
            enhanced_path = f"{base}_enhanced{ext}"
            if os.path.exists(enhanced_path):
                os.remove(enhanced_path)
        except Exception as e:
            logger.debug(f"Failed to cleanup enhanced image: {e}")

# Global OCR processor instance
ocr_processor = None

def get_ocr_processor() -> OCRProcessor:
    """Get singleton OCR processor instance"""
    global ocr_processor
    if ocr_processor is None:
        ocr_processor = OCRProcessor()
    return ocr_processor