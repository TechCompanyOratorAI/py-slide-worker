"""
Slide processing module for py-analysis-worker
Handles slide download, OCR processing, and embedding generation
"""

import os
import tempfile
import shutil
import logging
import requests
import atexit
from typing import Dict, Optional, List
from urllib.parse import urlparse

from src.clients.aws_client import get_aws_client
from src.processors.ocr_processor import get_ocr_processor
from config.config import check_library_availability
from config.memory_config import check_memory_usage, is_memory_available, optimize_memory

logger = logging.getLogger(__name__)

# Check library availability
LIBS = check_library_availability()

class SlideProcessor:
    """Main slide processor for handling slide files"""
    
    def __init__(self):
        """Initialize slide processor"""
        self.aws_client = get_aws_client()
        self.ocr_processor = get_ocr_processor()
        self.temp_directories = []  # Track temp directories for cleanup
        
        # Register cleanup function
        atexit.register(self.cleanup_all_temp_dirs)
        logger.info("Slide processor initialized")
    
    def download_slide(self, slide_url: str, local_path: str) -> str:
        """
        Download slide from S3 URL to local path.

        Returns:
            'ok'         – download succeeded
            'not_found'  – S3 object does not exist (job was deleted)
            'error'      – any other failure
        """
        try:
            # Parse S3 URL
            if slide_url.startswith('s3://'):
                parts = slide_url.replace('s3://', '').split('/', 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ''
                return self.aws_client.download_from_s3(bucket, key, local_path)

            elif slide_url.startswith('http'):
                parsed = urlparse(slide_url)
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
                    logger.info(f"Downloading from S3 via boto3: bucket={bucket}, key={key}")
                    return self.aws_client.download_from_s3(bucket, key, local_path)
                else:
                    logger.warning(f"Could not parse S3 URL, trying HTTP download: {slide_url}")
                    response = requests.get(slide_url, stream=True)
                    response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"Downloaded slide from HTTP: {slide_url} -> {local_path}")
                    return 'ok'
            else:
                logger.error(f"Unsupported URL format: {slide_url}")
                return 'error'

        except Exception as e:
            logger.error(f"Failed to download slide {slide_url}: {e}")
            return 'error'
    
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
    
    def cleanup_all_temp_dirs(self):
        """Cleanup all temporary directories"""
        for temp_dir in self.temp_directories[:]:  # Copy list to avoid modification during iteration
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Cleaned up temp directory: {temp_dir}")
                self.temp_directories.remove(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory {temp_dir}: {e}")
    
    def process_slide(self, slide_url: str, slide_id: int) -> Dict:
        """
        Process a single slide: download, smart text extraction, per-slide embeddings
        
        Args:
            slide_url: URL of slide file (PDF, image, etc.)
            slide_id: Slide ID
            
        Returns:
            Dictionary with processing results optimized for slide-speech alignment
        """
        result = {
            'success': False,
            'extractedText': None,  # Combined text (for backward compatibility)
            'slides': None,  # List of {slideIndex, text, embedding} for alignment
            'pages': None,  # Raw page data (for debugging)
            'totalSlides': 0,
            'error': None,
            'not_found': False,  # True when S3 object is gone (job deleted)
        }
        
        # Check memory before starting
        if not is_memory_available(150):  # Need ~150MB for slide processing
            logger.error("Insufficient memory for slide processing")
            result['error'] = 'Insufficient memory'
            return result
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix=f'slide_{slide_id}_')
        self.temp_directories.append(temp_dir)  # Track for cleanup
        
        # Determine file extension
        file_ext = self._determine_file_extension(slide_url)
        slide_path = os.path.join(temp_dir, f'slide_{slide_id}{file_ext}')
        
        try:
            # Monitor memory usage
            check_memory_usage()
            # Download slide
            download_status = self.download_slide(slide_url, slide_path)
            if download_status == 'not_found':
                result['error'] = 'Slide not found (job may have been deleted)'
                result['not_found'] = True
                return result
            elif download_status != 'ok':
                result['error'] = 'Failed to download slide'
                return result
            
            # Determine file type
            is_pdf = file_ext == '.pdf' or slide_path.lower().endswith('.pdf')
            is_pptx = file_ext in ('.pptx', '.ppt') or slide_path.lower().endswith(('.pptx', '.ppt'))

            if is_pptx:
                # Process PPTX with python-pptx direct text extraction
                logger.info(f"Processing PPTX file: {slide_path}")
                result = self._process_pptx(slide_path, result)
            elif is_pdf:
                # Process PDF with smart text-first extraction
                logger.info(f"Processing PDF file with smart extraction: {slide_path}")
                result = self._process_pdf_smart(slide_path, result)
            else:
                # Process as single image/slide
                logger.info(f"Processing single slide: {slide_path}")
                result = self._process_single_slide(slide_path, result)
            
            # Generate per-slide embeddings for alignment
            if result['success'] and result['slides']:
                result = self._generate_per_slide_embeddings(result)
            
            # Cleanup enhanced image if created (for single image files)
            if not is_pdf and not is_pptx:
                self.ocr_processor.cleanup_enhanced_image(slide_path)
            
        except MemoryError as e:
            logger.error(f"Slide processing failed due to memory exhaustion: {e}")
            result['error'] = f'Memory exhaustion: {str(e)}'
            optimize_memory()
        except Exception as e:
            logger.error(f"Error processing slide {slide_id}: {e}", exc_info=True)
            result['error'] = str(e)
        
        finally:
            # Cleanup temporary files
            try:
                if temp_dir in self.temp_directories:
                    self.temp_directories.remove(temp_dir)
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory {temp_dir}: {e}")
            
            # Force memory cleanup
            optimize_memory()
        
        return result
    
    def _process_pdf_smart(self, pdf_path: str, result: Dict) -> Dict:
        """
        Process PDF with smart text-first extraction + OCR fallback
        
        Args:
            pdf_path: Path to PDF file
            result: Result dictionary to update
            
        Returns:
            Updated result dictionary
        """
        try:
            # Import PDF text extractor
            from .pdf_text_extractor import get_pdf_text_extractor
            pdf_extractor = get_pdf_text_extractor(self.ocr_processor)
            
            # Smart extraction with OCR fallback
            pages_data = pdf_extractor.smart_extract_with_ocr_fallback(pdf_path)
            
            if not pages_data:
                result['error'] = 'Failed to extract text from PDF'
                return result
            
            # Convert pages to slides format (for alignment)
            slides_data = []
            all_text_parts = []
            
            for page_data in pages_data:
                slide_text = page_data['text'].strip()
                
                # Create slide entry for alignment
                slide_entry = {
                    'slideIndex': page_data['pageNumber'],  # 1-based index
                    'text': slide_text,
                    'extractionMethod': page_data['extractionMethod'],
                    'confidence': page_data['confidence'],
                    'embedding': None  # Will be filled later
                }
                slides_data.append(slide_entry)
                
                # For combined text (backward compatibility)
                if slide_text:
                    all_text_parts.append(f"[Slide {page_data['pageNumber']}]\n{slide_text}")
            
            # Update result
            result['slides'] = slides_data
            result['pages'] = pages_data  # Raw page data for debugging
            result['totalSlides'] = len(slides_data)
            result['extractedText'] = '\n\n'.join(all_text_parts)  # Combined for compatibility
            result['success'] = True
            
            logger.info(f"✅ Smart PDF processing: {len(slides_data)} slides extracted")
            
        except Exception as e:
            logger.error(f"Smart PDF processing failed: {e}")
            result['error'] = f'Smart PDF processing failed: {str(e)}'
        
        return result
    
    def _process_pptx(self, pptx_path: str, result: Dict) -> Dict:
        """
        Process PPTX/PPT file with three-phase strategy:
          Phase 1 (sequential): extract text frames + write image blobs to temp files
          Phase 2 (parallel):   OCR image-heavy slides concurrently via ThreadPoolExecutor
          Phase 3 (sequential): merge results in original slide order, build output
        """
        if not LIBS.get('PPTX_AVAILABLE'):
            result['error'] = 'python-pptx not installed. Cannot process PPTX files.'
            logger.error(result['error'])
            return result

        # PIL image formats that OCR can handle (skip EMF/WMF which PIL cannot open)
        SUPPORTED_IMG_EXTS = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'tif'}
        MIN_TEXT_CHARS = 20
        temp_dir = os.path.dirname(pptx_path)

        try:
            from pptx import Presentation
            from pptx.enum.shapes import MSO_SHAPE_TYPE
            from concurrent.futures import ThreadPoolExecutor, as_completed
            from config.memory_config import PAGE_WORKERS

            prs = Presentation(pptx_path)

            # ------------------------------------------------------------------
            # Phase 1: sequential — read XML + write image blobs to disk
            # ------------------------------------------------------------------
            slide_info_list = []  # one dict per slide, preserves original order

            for slide_index, slide in enumerate(prs.slides, start=1):
                # Extract direct text from all text frames
                text_parts = []
                for shape in slide.shapes:
                    if shape.has_text_frame:
                        for para in shape.text_frame.paragraphs:
                            para_text = ''.join(run.text for run in para.runs).strip()
                            if para_text:
                                text_parts.append(para_text)

                direct_text = '\n'.join(text_parts).strip()

                # Collect image blobs that need OCR (only when direct text is sparse)
                image_items = []  # list of temp_img_path strings
                if len(direct_text) < MIN_TEXT_CHARS and LIBS.get('OCR_AVAILABLE'):
                    for shape in slide.shapes:
                        if shape.shape_type != MSO_SHAPE_TYPE.PICTURE:
                            continue
                        try:
                            img_ext = shape.image.ext.lower()
                            if img_ext not in SUPPORTED_IMG_EXTS:
                                logger.debug(f"Slide {slide_index}: skipping unsupported image format '{img_ext}'")
                                continue

                            temp_img_path = os.path.join(
                                temp_dir,
                                f'pptx_slide{slide_index}_shape{shape.shape_id}.{img_ext}'
                            )
                            with open(temp_img_path, 'wb') as f:
                                f.write(shape.image.blob)
                            image_items.append(temp_img_path)

                        except Exception as e:
                            logger.warning(f"Slide {slide_index}: failed to write image blob for shape {shape.shape_id}: {e}")

                slide_info_list.append({
                    'slideIndex': slide_index,
                    'direct_text': direct_text,
                    'image_items': image_items,
                })

            # ------------------------------------------------------------------
            # Phase 2: parallel — OCR all image items for slides that need it
            # ocr_results[slide_index] = combined OCR text for that slide
            # ------------------------------------------------------------------
            def _ocr_slide_images(slide_info: Dict) -> tuple:
                """OCR all images for one slide; returns (slideIndex, combined_text)."""
                slide_idx = slide_info['slideIndex']
                ocr_texts = []
                for temp_img_path in slide_info['image_items']:
                    try:
                        ocr_text = self.ocr_processor.extract_text_from_image(temp_img_path)
                        if ocr_text:
                            ocr_texts.append(ocr_text.strip())
                            logger.debug(f"Slide {slide_idx} image '{temp_img_path}': OCR {len(ocr_text)} chars")
                    except Exception as e:
                        logger.warning(f"Slide {slide_idx}: OCR failed for '{temp_img_path}': {e}")
                    finally:
                        # Clean up temp image + enhanced variant
                        try:
                            if os.path.exists(temp_img_path):
                                os.remove(temp_img_path)
                            self.ocr_processor.cleanup_enhanced_image(temp_img_path)
                        except Exception:
                            pass
                return slide_idx, '\n'.join(ocr_texts)

            ocr_results: Dict[int, str] = {}
            slides_needing_ocr = [si for si in slide_info_list if si['image_items']]

            if slides_needing_ocr:
                logger.info(f"PPTX: running OCR on {len(slides_needing_ocr)} image-heavy slides "
                            f"(PAGE_WORKERS={PAGE_WORKERS})")
                with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
                    future_to_slide = {
                        executor.submit(_ocr_slide_images, si): si['slideIndex']
                        for si in slides_needing_ocr
                    }
                    for future in as_completed(future_to_slide):
                        slide_idx = future_to_slide[future]
                        try:
                            idx, combined_text = future.result()
                            ocr_results[idx] = combined_text
                        except Exception as e:
                            logger.error(f"Slide {slide_idx}: OCR worker error: {e}")
                            ocr_results[slide_idx] = ''

            # ------------------------------------------------------------------
            # Phase 3: sequential — merge in original order, build output
            # ------------------------------------------------------------------
            slides_data = []
            all_text_parts = []

            for si in slide_info_list:
                slide_index = si['slideIndex']
                direct_text = si['direct_text']
                ocr_combined = ocr_results.get(slide_index, '')

                if ocr_combined:
                    slide_text = (direct_text + '\n' + ocr_combined).strip() if direct_text else ocr_combined
                    extraction_method = 'pptx_ocr_fallback'
                    confidence = 'medium'
                    logger.info(f"Slide {slide_index}: OCR fallback → {len(ocr_combined)} chars")
                elif direct_text:
                    slide_text = direct_text
                    extraction_method = 'pptx_direct'
                    confidence = 'high'
                else:
                    slide_text = ''
                    extraction_method = 'pptx_direct'
                    confidence = 'low'
                    logger.debug(f"Slide {slide_index}: no text and no extractable images")

                slide_entry = {
                    'slideIndex': slide_index,
                    'text': slide_text,
                    'extractionMethod': extraction_method,
                    'confidence': confidence,
                    'embedding': None,
                }
                slides_data.append(slide_entry)

                if slide_text:
                    all_text_parts.append(f"[Slide {slide_index}]\n{slide_text}")

            if not slides_data:
                result['error'] = 'No slides found in PPTX file'
                return result

            result['slides'] = slides_data
            result['pages'] = [
                {
                    'pageNumber': s['slideIndex'],
                    'text': s['text'],
                    'extractionMethod': s['extractionMethod'],
                    'confidence': s['confidence'],
                }
                for s in slides_data
            ]
            result['totalSlides'] = len(slides_data)
            result['extractedText'] = '\n\n'.join(all_text_parts)
            result['success'] = True

            text_slides = sum(1 for s in slides_data if s['text'])
            logger.info(f"✅ PPTX: {len(slides_data)} slides, {text_slides} with text")

        except Exception as e:
            logger.error(f"PPTX processing failed: {e}", exc_info=True)
            result['error'] = f'PPTX processing failed: {str(e)}'

        return result

    def _process_single_slide(self, slide_path: str, result: Dict) -> Dict:
        """
        Process single image/slide file
        
        Args:
            slide_path: Path to slide file
            result: Result dictionary to update
            
        Returns:
            Updated result dictionary
        """
        try:
            # OCR single image
            extracted_text = self.ocr_processor.extract_text_from_image(slide_path)
            
            if extracted_text:
                # Create single slide entry
                slide_entry = {
                    'slideIndex': 1,
                    'text': extracted_text.strip(),
                    'extractionMethod': 'ocr',
                    'confidence': 'medium',
                    'embedding': None  # Will be filled later
                }
                
                result['slides'] = [slide_entry]
                result['pages'] = [{
                    'pageNumber': 1,
                    'text': extracted_text,
                    'extractionMethod': 'ocr',
                    'confidence': 'medium'
                }]
                result['totalSlides'] = 1
                result['extractedText'] = extracted_text  # Single slide text
                result['success'] = True
                
                logger.info(f"✅ Single slide processed: {len(extracted_text)} characters")
            else:
                result['error'] = 'OCR extraction failed for single slide'
                
        except Exception as e:
            logger.error(f"Single slide processing failed: {e}")
            result['error'] = f'Single slide processing failed: {str(e)}'
        
        return result
    
    def _generate_per_slide_embeddings(self, result: Dict) -> Dict:
        """
        Generate embeddings for each slide individually (for alignment)
        
        Args:
            result: Result dictionary with slides data
            
        Returns:
            Updated result with per-slide embeddings
        """
        try:
            if not result.get('slides'):
                return result
            
            slides_with_embeddings = []
            
            for slide_data in result['slides']:
                slide_text = slide_data['text'].strip()
                
                if slide_text:
                    # Generate embedding for this specific slide
                    embedding = self.generate_embedding(slide_text)
                    slide_data['embedding'] = embedding
                    
                    if embedding:
                        logger.debug(f"Generated embedding for slide {slide_data['slideIndex']}: {len(embedding)} dimensions")
                else:
                    slide_data['embedding'] = None
                    logger.debug(f"No text for slide {slide_data['slideIndex']}, no embedding generated")
                
                slides_with_embeddings.append(slide_data)
            
            result['slides'] = slides_with_embeddings
            
            # Count successful embeddings
            embedded_slides = sum(1 for s in slides_with_embeddings if s['embedding'] is not None)
            logger.info(f"✅ Generated embeddings for {embedded_slides}/{len(slides_with_embeddings)} slides")
            
        except Exception as e:
            logger.error(f"Per-slide embedding generation failed: {e}")
            # Don't fail the whole process, just log the error
        
        return result

# Global slide processor instance
slide_processor = None

def get_slide_processor() -> SlideProcessor:
    """Get singleton slide processor instance"""
    global slide_processor
    if slide_processor is None:
        slide_processor = SlideProcessor()
    return slide_processor