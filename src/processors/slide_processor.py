"""
Slide processing module for py-analysis-worker
Handles slide download, OCR processing, and embedding generation
"""

import os
import tempfile
import shutil
import logging
import requests
from typing import Dict, Optional, List
from urllib.parse import urlparse

from src.clients.aws_client import get_aws_client
from src.processors.ocr_processor import get_ocr_processor
from config.config import check_library_availability

logger = logging.getLogger(__name__)

# Check library availability
LIBS = check_library_availability()

class SlideProcessor:
    """Main slide processor for handling slide files"""
    
    def __init__(self):
        """Initialize slide processor"""
        self.aws_client = get_aws_client()
        self.ocr_processor = get_ocr_processor()
        logger.info("Slide processor initialized")
    
    def download_slide(self, slide_url: str, local_path: str) -> bool:
        """
        Download slide from S3 URL to local path
        
        Args:
            slide_url: S3 URL or HTTP URL
            local_path: Local file path to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Parse S3 URL
            if slide_url.startswith('s3://'):
                # s3://bucket/key
                parts = slide_url.replace('s3://', '').split('/', 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ''
                
                return self.aws_client.download_from_s3(bucket, key, local_path)
                
            elif slide_url.startswith('http'):
                # HTTP URL - try to parse S3 URL from it
                # Format: https://bucket.s3.region.amazonaws.com/key
                # or: https://s3.region.amazonaws.com/bucket/key
                parsed = urlparse(slide_url)
                
                # Try to extract bucket and key from S3 URL
                bucket = None
                key = None
                
                # Pattern 1: https://bucket.s3.region.amazonaws.com/key
                if '.s3.' in parsed.netloc:
                    bucket = parsed.netloc.split('.s3.')[0]
                    key = parsed.path.lstrip('/')
                # Pattern 2: https://s3.region.amazonaws.com/bucket/key
                elif 's3.' in parsed.netloc and parsed.path:
                    parts = parsed.path.lstrip('/').split('/', 1)
                    if len(parts) >= 2:
                        bucket = parts[0]
                        key = parts[1]
                
                if bucket and key:
                    # Download using boto3 (with credentials)
                    logger.info(f"Downloading from S3 via boto3: bucket={bucket}, key={key}")
                    return self.aws_client.download_from_s3(bucket, key, local_path)
                else:
                    # Fallback: try HTTP download (may fail if file is private)
                    logger.warning(f"Could not parse S3 URL, trying HTTP download: {slide_url}")
                    response = requests.get(slide_url, stream=True)
                    response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"Downloaded slide from HTTP: {slide_url} -> {local_path}")
                    return True
            else:
                logger.error(f"Unsupported URL format: {slide_url}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to download slide {slide_url}: {e}")
            return False
    
    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding vector for text
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector or None if failed
        """
        try:
            # TODO: Implement embedding generation
            # Example using sentence-transformers:
            # from sentence_transformers import SentenceTransformer
            # model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            # embedding = model.encode(text).tolist()
            
            logger.info(f"Generating embedding for text (length: {len(text)})")
            # Placeholder - return empty list for now
            embedding = []
            
            logger.info(f"Generated embedding vector of length {len(embedding)}")
            return embedding
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return None
    
    def _determine_file_extension(self, slide_url: str) -> str:
        """
        Determine file extension from URL
        
        Args:
            slide_url: URL of the slide file
            
        Returns:
            File extension (with dot)
        """
        # Determine file extension from URL or content type
        # Try to detect from URL first
        url_path = slide_url.split('?')[0]  # Remove query params
        file_ext = os.path.splitext(url_path)[1].lower()
        
        # If no extension or unknown, default to pdf (most common)
        if not file_ext or file_ext not in ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.pptx', '.ppt']:
            # Check if URL contains 'pdf' or content-type suggests PDF
            if 'pdf' in slide_url.lower() or 'application/pdf' in slide_url.lower():
                file_ext = '.pdf'
            else:
                file_ext = '.pdf'  # Default to PDF
        
        return file_ext
    
    def process_slide(self, slide_url: str, slide_id: int) -> Dict:
        """
        Process a single slide: download, OCR (handle PDF multi-page), generate embedding
        
        Args:
            slide_url: URL of slide file (PDF, image, etc.)
            slide_id: Slide ID
            
        Returns:
            Dictionary with processing results including pages data
        """
        result = {
            'success': False,
            'extractedText': None,  # Combined text from all pages
            'pages': None,  # List of {pageNumber, text} for multi-page files
            'embedding': None,
            'error': None
        }
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix=f'slide_{slide_id}_')
        
        # Determine file extension
        file_ext = self._determine_file_extension(slide_url)
        slide_path = os.path.join(temp_dir, f'slide_{slide_id}{file_ext}')
        
        try:
            # Download slide
            if not self.download_slide(slide_url, slide_path):
                result['error'] = 'Failed to download slide'
                return result
            
            # Check if it's a PDF file
            is_pdf = file_ext == '.pdf' or slide_path.lower().endswith('.pdf')
            
            if is_pdf and LIBS['PDF2IMAGE_AVAILABLE']:
                # Process PDF: extract text from each page
                logger.info(f"Processing PDF file: {slide_path}")
                pages_data = self.ocr_processor.process_pdf_to_pages(slide_path)
                
                if pages_data:
                    # Combine all text with Vietnamese format
                    combined_text = '\n\n'.join([f"[Trang {p['pageNumber']}]\n{p['text']}" for p in pages_data])
                    
                    result['extractedText'] = combined_text
                    result['pages'] = pages_data
                    result['success'] = True
                    
                    logger.info(f"✅ Processed {len(pages_data)} pages from PDF")
                else:
                    result['error'] = 'No text extracted from PDF'
            else:
                # Process as single image
                logger.info(f"Processing image file: {slide_path}")
                extracted_text = self.ocr_processor.extract_text_from_image(slide_path)
                
                if extracted_text:
                    result['extractedText'] = extracted_text
                    result['pages'] = [{
                        'pageNumber': 1,
                        'text': extracted_text
                    }]
                    result['success'] = True
                else:
                    result['error'] = 'OCR extraction failed'
            
            # Generate embedding if we have text
            if result['success'] and result['extractedText']:
                embedding = self.generate_embedding(result['extractedText'])
                if embedding:
                    result['embedding'] = embedding
            
            # Cleanup enhanced image if created (for single image files)
            if not is_pdf:
                self.ocr_processor.cleanup_enhanced_image(slide_path)
            
        except Exception as e:
            logger.error(f"Error processing slide {slide_id}: {e}", exc_info=True)
            result['error'] = str(e)
        
        finally:
            # Cleanup temporary files
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory {temp_dir}: {e}")
        
        return result

# Global slide processor instance
slide_processor = None

def get_slide_processor() -> SlideProcessor:
    """Get singleton slide processor instance"""
    global slide_processor
    if slide_processor is None:
        slide_processor = SlideProcessor()
    return slide_processor