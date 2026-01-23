"""
OCR processing module for py-analysis-worker
Handles text extraction from images and PDFs using pytesseract and easyocr
"""

import os
import logging
import signal
import time
from typing import Optional, List, Dict
from config.config import OCR_LANGUAGE, check_library_availability
from config.memory_config import check_memory_usage, is_memory_available, optimize_memory, MAX_IMAGE_SIZE

logger = logging.getLogger(__name__)

# Check library availability
LIBS = check_library_availability()

# Import libraries if available
if LIBS['OCR_AVAILABLE']:
    import pytesseract
    from PIL import Image, ImageEnhance, ImageFilter

if LIBS['PDF2IMAGE_AVAILABLE']:
    from pdf2image import convert_from_path, convert_from_bytes

if LIBS['CV2_AVAILABLE']:
    import cv2
    import numpy as np

# Commented out EasyOCR for lighter deployment
# if LIBS['EASYOCR_AVAILABLE']:
#     import easyocr

class OCRProcessor:
    """OCR processor for extracting text from images and PDFs"""
    
    def __init__(self):
        """Initialize OCR processor"""
        self.easyocr_reader = None
        
        # Skip EasyOCR for lighter deployment - use only pytesseract
        # if LIBS['EASYOCR_AVAILABLE']:
        #     try:
        #         # Initialize with Vietnamese and English
        #         self.easyocr_reader = easyocr.Reader(['vi', 'en'], gpu=False)
        #         logger.info("✅ EasyOCR initialized (Vietnamese + English)")
        #     except Exception as e:
        #         logger.warning(f"⚠️ Failed to initialize EasyOCR: {e}. Will use pytesseract.")
        #         self.easyocr_reader = None
        
        logger.info("OCR processor initialized")
    
    def enhance_image(self, image_path: str) -> Optional[str]:
        """
        Enhance image quality for better OCR results
        
        Args:
            image_path: Path to original image
            
        Returns:
            Path to enhanced image or None if failed
        """
        if not LIBS['CV2_AVAILABLE']:
            return image_path  # Return original if cv2 not available
        
        try:
            # Read image
            img = cv2.imread(image_path)
            if img is None:
                return image_path
            
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Apply denoising
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            
            # Increase contrast using CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(denoised)
            
            # Apply threshold to get binary image
            _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            # Save enhanced image
            enhanced_path = image_path.replace('.', '_enhanced.')
            cv2.imwrite(enhanced_path, binary)
            
            logger.debug(f"Enhanced image saved: {enhanced_path}")
            return enhanced_path
            
        except Exception as e:
            logger.warning(f"Image enhancement failed: {e}. Using original image.")
            return image_path
    
    def extract_text_from_image(self, image_path: str) -> Optional[str]:
        """
        Extract text from single image using OCR with memory management
        
        Args:
            image_path: Path to image file
            
        Returns:
            Extracted text or None if failed
        """
        if not LIBS['OCR_AVAILABLE']:
            logger.error("OCR libraries not available. Please install pytesseract and Pillow.")
            return None
        
        # Check file size
        try:
            file_size = os.path.getsize(image_path)
            if file_size > MAX_IMAGE_SIZE:
                logger.error(f"Image too large: {file_size} bytes > {MAX_IMAGE_SIZE} bytes limit")
                return None
        except Exception as e:
            logger.warning(f"Could not check file size: {e}")
        
         # Check memory before processing (reduced requirement)
         if not is_memory_available(100):  # Reduced to 100MB for OCR
             logger.error("Insufficient memory for OCR processing")
             return None
        
        try:
            # Monitor memory during processing
            check_memory_usage()
            # Enhance image for better OCR
            enhanced_path = self.enhance_image(image_path)
            
            # Try EasyOCR first (better for Vietnamese)
            if self.easyocr_reader:
                try:
                    logger.info(f"Using EasyOCR for: {image_path}")
                    results = self.easyocr_reader.readtext(enhanced_path if enhanced_path != image_path else image_path)
                    
                    # Combine all detected text
                    text_parts = []
                    for (bbox, text, confidence) in results:
                        if confidence > 0.5:  # Filter low confidence results
                            text_parts.append(text)
                    
                    text = '\n'.join(text_parts)
                    
                    if text.strip():
                        logger.info(f"EasyOCR extracted {len(text)} characters (confidence filtered)")
                        return text.strip()
                    else:
                        logger.warning("EasyOCR returned empty text, falling back to pytesseract")
                except Exception as e:
                    logger.warning(f"EasyOCR failed: {e}. Falling back to pytesseract.")
            
            # Fallback to pytesseract
            logger.info(f"Using pytesseract for: {image_path}")
            
            # Open image
            image = Image.open(enhanced_path if enhanced_path != image_path else image_path)
            
            # OCR configuration for better Vietnamese support
            # PSM 6: Assume a single uniform block of text
            # PSM 11: Sparse text (for slides with scattered text)
            # Try PSM 6 first, then 11 if needed
            
            # Try different PSM modes for better results with timeout
            configs = [
                f'--psm 6 -l {OCR_LANGUAGE}',  # Uniform block
                f'--psm 11 -l {OCR_LANGUAGE}',  # Sparse text
                f'--psm 3 -l {OCR_LANGUAGE}',   # Fully automatic
            ]
            
            best_text = None
            for i, config in enumerate(configs):
                try:
                     # Check memory before each attempt (reduced requirement)
                     if not is_memory_available(30):
                         logger.warning(f"Skipping OCR config {i+1} due to low memory")
                         break
                    
                    # Set timeout for OCR operation (30 seconds max)
                    def timeout_handler(signum, frame):
                        raise TimeoutError("OCR operation timed out")
                    
                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(30)  # 30 second timeout
                    
                    try:
                        text = pytesseract.image_to_string(image, config=config)
                        text = ' '.join(text.split())  # Clean up whitespace
                        
                        if text.strip() and (best_text is None or len(text) > len(best_text)):
                            best_text = text
                    finally:
                        signal.alarm(0)  # Cancel timeout
                        
                except TimeoutError:
                    logger.warning(f"OCR config {i+1} timed out, trying next...")
                    continue
                except Exception as e:
                    logger.warning(f"OCR config {i+1} failed: {e}")
                    continue
                finally:
                    # Force cleanup after each attempt
                    optimize_memory()
            
            if best_text and best_text.strip():
                logger.info(f"Pytesseract extracted {len(best_text)} characters")
                return best_text.strip()
            else:
                logger.warning("Pytesseract returned empty text")
                return None
                
        except MemoryError as e:
            logger.error(f"OCR failed due to memory exhaustion for {image_path}: {e}")
            optimize_memory()
            return None
        except Exception as e:
            logger.error(f"OCR failed for {image_path}: {e}", exc_info=True)
            optimize_memory()
            return None
        finally:
            # Always cleanup memory after OCR
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
            enhanced_path = original_path.replace('.', '_enhanced.')
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