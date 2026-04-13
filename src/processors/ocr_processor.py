"""
OCR processing module for py-analysis-worker
Handles text extraction from images and PDFs using pytesseract and easyocr
"""

import os
import logging
import signal
import threading
import time
from typing import Optional, List, Dict
from config.config import OCR_LANGUAGE, check_library_availability
from config.memory_config import check_memory_usage, is_memory_available, optimize_memory, MAX_IMAGE_SIZE, _ocr_semaphore

logger = logging.getLogger(__name__)

# Auto-detect and set TESSDATA_PREFIX if not already configured
def _setup_tessdata_prefix():
    """
    Detect the correct tessdata directory and set TESSDATA_PREFIX.

    Search order:
      1. Explicit env var (skip if already set)
      2. DigitalOcean buildpack path  (/layers/digitalocean_apt/apt/...)
      3. Standard distro paths
      4. Glob scan for any installed tesseract version (4.00, 4.1, 5.x, etc.)
    """
    if os.environ.get('TESSDATA_PREFIX'):
        existing = os.environ['TESSDATA_PREFIX']
        logger.info(f"TESSDATA_PREFIX already set: {existing}")
        return

    import glob

    candidate_dirs = [
        # DigitalOcean buildpack apt layer (highest priority — confirmed working)
        '/layers/digitalocean_apt/apt/usr/share/tesseract-ocr/4.00/tessdata',
        '/layers/digitalocean_apt/apt/usr/share/tesseract-ocr/5.00/tessdata',
        # Standard paths with exact version
        '/usr/share/tesseract-ocr/4.00/tessdata',
        '/usr/share/tesseract-ocr/5.00/tessdata',
        '/usr/share/tesseract-ocr/5/tessdata',
        '/usr/share/tesseract-ocr/4/tessdata',
        '/usr/share/tessdata',
        '/usr/local/share/tessdata',
        '/opt/homebrew/share/tessdata',
    ]

    # Append glob-discovered paths (catches any installed version: 4.1, 5.3, etc.)
    for pattern in [
        '/layers/digitalocean_apt/apt/usr/share/tesseract-ocr/*/tessdata',
        '/usr/share/tesseract-ocr/*/tessdata',
        '/usr/local/share/tesseract-ocr/*/tessdata',
    ]:
        for discovered in sorted(glob.glob(pattern), reverse=True):  # newest first
            if discovered not in candidate_dirs:
                candidate_dirs.append(discovered)

    for path in candidate_dirs:
        try:
            if not os.path.isdir(path):
                continue
            traineddata_files = [f for f in os.listdir(path) if f.endswith('.traineddata')]
            if traineddata_files:
                os.environ['TESSDATA_PREFIX'] = path
                langs = [f.replace('.traineddata', '') for f in traineddata_files[:5]]
                logger.info(
                    f"Auto-detected TESSDATA_PREFIX: {path} "
                    f"(languages: {langs}{'...' if len(traineddata_files) > 5 else ''})"
                )
                return
        except Exception as e:
            logger.debug(f"Skipping tessdata candidate {path}: {e}")

    logger.error(
        "❌ Could not auto-detect tessdata directory. "
        "Set TESSDATA_PREFIX environment variable manually. "
        "Run: find / -name 'eng.traineddata' 2>/dev/null"
    )

_setup_tessdata_prefix()

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

            # Detect dark-background slides (white text on dark bg) and invert
            # so text is always dark on light background before thresholding
            if gray.mean() < 128:
                gray = cv2.bitwise_not(gray)

            # Apply denoising
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)

            # Increase contrast using CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(denoised)

            # Apply threshold to get binary image
            _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

            # Save enhanced image (fix: use splitext to handle filenames with multiple dots)
            base, ext = os.path.splitext(image_path)
            enhanced_path = f"{base}_enhanced{ext}"
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

        # Semaphore caps total concurrent OCR across all threads/jobs
        # preventing OOM when intra-job and inter-job parallelism overlap
        with _ocr_semaphore:
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

                # Try different PSM modes for better results with timeout
                configs = [
                    f'--psm 6 -l {OCR_LANGUAGE}',  # Uniform block of text
                    f'--psm 11 -l {OCR_LANGUAGE}',  # Sparse text
                    f'--psm 3 -l {OCR_LANGUAGE}',   # Fully automatic page seg
                ]

                best_text = None
                failed_configs = []
                for i, config in enumerate(configs):
                    try:
                        # Check memory before each attempt
                        if not is_memory_available(30):
                            logger.warning(f"Skipping OCR config {i+1} due to low memory")
                            break

                        # -------------------------------------------------------
                        # Timeout strategy:
                        #   Main thread  → SIGALRM provides a hard 20-second
                        #                  deadline via the OS signal mechanism.
                        #   Worker thread → SIGALRM is silently ignored by the OS
                        #                  (signals always fire on the main thread).
                        #                  Callers running OCR inside a
                        #                  ThreadPoolExecutor MUST enforce the
                        #                  deadline externally by calling
                        #                  future.result(timeout=<secs>).
                        # -------------------------------------------------------
                        _in_main = threading.current_thread() is threading.main_thread()
                        if _in_main:
                            def _timeout_handler(_signum, _frame):
                                raise TimeoutError("OCR operation timed out")
                            signal.signal(signal.SIGALRM, _timeout_handler)
                            signal.alarm(20)  # Reduced from 30s → 20s per config

                        try:
                            text = pytesseract.image_to_string(image, config=config)
                            text = ' '.join(text.split())

                            if text.strip() and (best_text is None or len(text) > len(best_text)):
                                best_text = text

                            # Early exit if result is good enough
                            if best_text and len(best_text) >= 100:
                                break
                        finally:
                            if _in_main:
                                signal.alarm(0)

                    except TimeoutError:
                        logger.debug(f"OCR config {i+1} timed out")
                        failed_configs.append(f'psm_{config.split()[1]}_timeout')
                        continue
                    except Exception as e:
                        # Collect failures silently; log once at the end
                        failed_configs.append(f'psm_{config.split()[1]}')
                        logger.debug(f"OCR config {i+1} failed: {e}")
                        continue
                    finally:
                        optimize_memory()

                # Log all failures in one message instead of per-config spam
                if failed_configs:
                    logger.warning(
                        f"OCR configs failed for {image_path}: {failed_configs}. "
                        f"Check TESSDATA_PREFIX={os.environ.get('TESSDATA_PREFIX', 'NOT SET')}"
                    )

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