"""
OCR processing module for py-analysis-worker
Handles text extraction from images and PDFs using pytesseract and easyocr
"""

import os
import logging
from typing import Optional, List, Dict
from config.config import OCR_LANGUAGE, check_library_availability

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
        Extract text from single image using OCR
        
        Args:
            image_path: Path to image file
            
        Returns:
            Extracted text or None if failed
        """
        if not LIBS['OCR_AVAILABLE']:
            logger.error("OCR libraries not available. Please install pytesseract and Pillow.")
            return None
        
        try:
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
            
            # Try different PSM modes for better results
            configs = [
                f'--psm 6 -l {OCR_LANGUAGE}',  # Uniform block
                f'--psm 11 -l {OCR_LANGUAGE}',  # Sparse text
                f'--psm 3 -l {OCR_LANGUAGE}',   # Fully automatic
            ]
            
            best_text = None
            for config in configs:
                try:
                    text = pytesseract.image_to_string(image, config=config)
                    text = ' '.join(text.split())  # Clean up whitespace
                    
                    if text.strip() and (best_text is None or len(text) > len(best_text)):
                        best_text = text
                except:
                    continue
            
            if best_text and best_text.strip():
                logger.info(f"Pytesseract extracted {len(best_text)} characters")
                return best_text.strip()
            else:
                logger.warning("Pytesseract returned empty text")
                return None
                
        except Exception as e:
            logger.error(f"OCR failed for {image_path}: {e}", exc_info=True)
            return None
    
    def process_pdf_to_pages(self, pdf_path: str) -> List[Dict]:
        """
        Process PDF file: convert each page to image and extract text
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of dictionaries with pageNumber and text
        """
        if not LIBS['PDF2IMAGE_AVAILABLE'] or not LIBS['OCR_AVAILABLE']:
            logger.error("PDF processing libraries not available")
            return []
        
        try:
            pages_data = []
            
            # Convert PDF to images (one image per page) with higher DPI for better quality
            logger.info(f"Converting PDF to images: {pdf_path}")
            # Use optimal DPI (250) for better quality with 1GB RAM
            images = convert_from_path(pdf_path, dpi=250, fmt='png')
            
            logger.info(f"PDF has {len(images)} pages")
            
            # Process each page
            for page_num, image in enumerate(images, start=1):
                # Save temporary image
                temp_image_path = pdf_path.replace('.pdf', f'_page_{page_num}.png')
                image.save(temp_image_path, 'PNG', quality=85, optimize=True)
                
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
                
                # Cleanup temp image and enhanced image if exists
                try:
                    os.remove(temp_image_path)
                    enhanced_path = temp_image_path.replace('.png', '_enhanced.png')
                    if os.path.exists(enhanced_path):
                        os.remove(enhanced_path)
                except Exception as e:
                    logger.debug(f"Failed to cleanup temp files: {e}")
            
            return pages_data
            
        except Exception as e:
            logger.error(f"PDF processing failed: {e}", exc_info=True)
            return []
    
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