#!/usr/bin/env python3
"""
Book Spine Extractor using Google Gemini API
Processes video frames to extract book information using Gemini Vision model.
"""

import os
import json
import cv2
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from google import genai
from PIL import Image
import time
import io
from deduplicator import BookDeduplicator

class GeminiBookExtractor:
    """Extract book information from images using Gemini Vision API."""
    
    def __init__(self, api_key: str = None, model_name: str = "gemini-3-flash-preview", rescale: int = None, vertexai: bool = False, project: str = None, location: str = "us-central1"):
        """
        Initialize the Gemini or Vertex AI client.
        
        Args:
            api_key: Google API key (for Gemini AI Studio)
            model_name: Model name or path (e.g. gemini-3-flash-preview or publishers/meta/models/llama-3.2-90b-vision-instruct)
            rescale: Maximum dimension to rescale images to (None to disable)
            vertexai: Whether to use Vertex AI instead of Gemini AI Studio
            project: GCP Project ID (required for Vertex AI)
            location: GCP Location (default: us-central1)
        """
        if vertexai:
            if not project:
                raise ValueError("GCP Project ID required for Vertex AI (--project).")
            # Initialize client with Vertex AI configuration
            self.client = genai.Client(vertexai=True, project=project, location=location)
            print(f"Initialized Vertex AI client in {location} for project {project}")
        else:
            # Get API key from parameter or environment
            self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
            if not self.api_key:
                raise ValueError(
                    "Google API key required. Set GOOGLE_API_KEY environment variable "
                    "or pass api_key parameter."
                )
            # Initialize client with Gemini AI Studio configuration
            self.client = genai.Client(api_key=self.api_key)
            print("Initialized Gemini AI Studio client")
        
        # Initialize model and rescale settings
        self.model_name = model_name
        self.rescale = rescale
        
        # Token counters
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        
        print(f"Initialized Gemini model: {model_name}")
        if self.rescale:
            print(f"Rescale enabled: max dimension {self.rescale}px")
        
    async def extract_books_from_image(self, image_path: str) -> Dict[str, Any]:
        """
        Extract book information from a single image file.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dictionary containing extracted book information
        """
        with open(image_path, 'rb') as f:
            image_bytes = f.read()
        return await self.extract_books_from_image_bytes(image_bytes)

    async def extract_books_from_image_bytes(self, image_bytes: bytes) -> Dict[str, Any]:
        """
        Extract book information from image bytes.
        
        Args:
            image_bytes: Bytes of the image
            
        Returns:
            Dictionary containing extracted book information
        """
        # Load image from bytes
        image = Image.open(io.BytesIO(image_bytes))
        w, h = image.size
        
        # Rescale if enabled
        if self.rescale and max(w, h) > self.rescale:
            scale = self.rescale / max(w, h)
            new_size = (int(w * scale), int(h * scale))
            print(f"    Resizing image from {w}x{h} to {new_size[0]}x{new_size[1]}...")
            image = image.resize(new_size, Image.LANCZOS)
        else:
            print(f"    Image size: {w}x{h}")
        
        # Create prompt for structured extraction
        prompt = """Analyze this image of a bookshelf and extract information about all visible books.
Check for books in vertical, horizontal, and diagnoal positions.
For each book, identify:
- Title (if visible on the spine)
- Author (if visible)
- Publisher (if visible)
- Year (if visible)
- Other text visible on the spine

Return the information as a pipe-separated table, one book per line.
Header: title | author | publisher | year | other_text

If a field is not visible, use "null".
If you see a book but cannot read it, include it with "null" values.

Return the books in the order they appear from left to right (for vertically stacked books), and top to bottom (for horiozontally stacked books)

Only return the table, no other text. Do not include markdown code blocks."""

        # Generate response
        print("    Running inference...")
        start_time = time.time()
        try:
            response = await self.client.aio.models.generate_content(
                model=self.model_name,
                contents=[prompt, image],
                config={
                    'temperature': 0.1,
                    'max_output_tokens': 8000,
                }
            )
            
            elapsed = time.time() - start_time
            print(f"    Inference completed in {elapsed:.2f}s")
            
            output_text = response.text
            
            # Extract token usage
            usage = response.usage_metadata
            prompt_tokens = usage.prompt_token_count
            completion_tokens = usage.candidates_token_count
            
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += completion_tokens
            
            # Parse pipe-separated response
            books = []
            try:
                lines = output_text.strip().split('\n')
                for line in lines:
                    if '|' not in line:
                        continue
                    if 'title | author' in line.lower():
                        continue
                    
                    parts = [p.strip() for p in line.split('|')]
                    # Ensure we have at least 5 parts (pad with null if needed)
                    while len(parts) < 5:
                        parts.append("null")
                    
                    book = {
                        "title": parts[0] if parts[0].lower() != "null" else None,
                        "author": parts[1] if parts[1].lower() != "null" else None,
                        "publisher": parts[2] if parts[2].lower() != "null" else None,
                        "year": parts[3] if parts[3].lower() != "null" else None,
                        "other_text": parts[4] if parts[4].lower() != "null" else None
                    }
                    books.append(book)
                
                result = {
                    "books": books,
                    "usage": {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_tokens": prompt_tokens + completion_tokens
                    }
                }
                
            except Exception as e:
                result = {
                    "books": [],
                    "raw_response": output_text,
                    "parse_error": str(e)
                }
                
        except Exception as e:
            result = {
                "books": [],
                "error": str(e)
            }
        
        return result


async def process_video(
    video_path: str,
    output_dir: str,
    frame_interval: int = 30,
    api_key: str = None,
    model_name: str = "gemini-3-flash-preview",
    rescale: int = None,
    vertexai: bool = False,
    project: str = None,
    location: str = "us-central1"
):
    """
    Process a video file and extract book information from frames.
    
    Args:
        video_path: Path to input video
        output_dir: Directory to save outputs
        frame_interval: Process every Nth frame
        api_key: Google API key
        model_name: Gemini model to use
        rescale: Rescale images to this max dimension
        vertexai: Whether to use Vertex AI
        project: GCP Project ID
        location: GCP Location
    """
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    frames_dir = output_dir / "frames"
    frames_dir.mkdir(exist_ok=True)
    
    print(f"Processing video: {video_path}")
    print(f"Frame interval: every {frame_interval} frames")
    print(f"Output directory: {output_dir}\n")
    
    # Initialize extractor
    extractor = GeminiBookExtractor(
        api_key=api_key, 
        model_name=model_name, 
        rescale=rescale,
        vertexai=vertexai,
        project=project,
        location=location
    )
    
    # Open video
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"Error: Could not open video {video_path}")
        return
    
    # Get video properties
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    
    print(f"Video info: {total_frames} frames @ {fps:.2f} FPS")
    print(f"Will process ~{total_frames // frame_interval} frames\n")
    
    frame_count = 0
    processed_count = 0
    all_results = []
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        # Process every Nth frame
        if frame_count % frame_interval == 0:
            print(f"\n{'='*60}")
            print(f"Processing frame {frame_count}/{total_frames} (#{processed_count + 1})")
            print(f"{'='*60}")
            
            # Save frame
            frame_filename = f"frame_{frame_count:06d}.jpg"
            frame_path = frames_dir / frame_filename
            cv2.imwrite(str(frame_path), frame)
            
            # Extract books from frame
            result = await extractor.extract_books_from_image(str(frame_path))
            
            # Add metadata
            result["frame_number"] = frame_count
            result["timestamp_seconds"] = frame_count / fps
            result["frame_path"] = str(frame_path)
            
            all_results.append(result)
            
            # Print summary
            if "books" in result and result["books"]:
                print(f"  Found {len(result['books'])} books")
                for i, book in enumerate(result["books"], 1):
                    title = book.get("title", "Unknown")
                    author = book.get("author", "Unknown")
                    print(f"    {i}. {title} by {author}")
                
                if "usage" in result:
                    u = result["usage"]
                    print(f"  Tokens: {u['prompt_tokens']} in / {u['completion_tokens']} out (Total: {u['total_tokens']})")
            else:
                print(f"  No books found or parse error")
                # Show debug info
                if "parse_error" in result:
                    print(f"  Parse error: {result['parse_error']}")
                if "raw_response" in result:
                    print(f"  Raw response: {result['raw_response'][:500]}...")
                if "error" in result:
                    print(f"  Error: {result['error']}")
            
            processed_count += 1
            
            # Rate limiting: Gemini has API limits
            time.sleep(0.5)  # Small delay to avoid hitting rate limits
        
        frame_count += 1
    
    cap.release()
    
    # Save results
    output_json = output_dir / "extracted_books.json"
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    # Create summary
    total_books = sum(len(r.get("books", [])) for r in all_results)
    print(f"\n{'='*60}")
    print(f"Video processing complete!")
    print(f"Processed {processed_count} frames")
    print(f"Total books found: {total_books}")
    print(f"Total Token Usage:")
    print(f"  Input (Prompt): {extractor.total_prompt_tokens}")
    print(f"  Output (Completion): {extractor.total_completion_tokens}")
    print(f"  Combined Total: {extractor.total_prompt_tokens + extractor.total_completion_tokens}")
    print(f"Results saved to: {output_json}")
    print(f"{'='*60}")


async def process_image(
    image_path: str,
    output_path: str,
    api_key: str = None,
    model_name: str = "gemini-3-flash-preview",
    rescale: int = None,
    vertexai: bool = False,
    project: str = None,
    location: str = "us-central1"
):
    """
    Process a single image file and extract book information.
    
    Args:
        image_path: Path to input image
        output_path: Path to save JSON output
        api_key: Google API key
        model_name: Gemini model to use
        rescale: Rescale image to this max dimension
        vertexai: Whether to use Vertex AI
        project: GCP Project ID
        location: GCP Location
    """
    print(f"Processing image: {image_path}\n")
    
    with open(image_path, 'rb') as f:
        image_bytes = f.read()
        
    result = process_image_bytes(
        image_bytes=image_bytes,
        api_key=api_key,
        model_name=model_name,
        rescale=rescale,
        vertexai=vertexai,
        project=project,
        location=location
    )
    
    # Save results
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_path}")


async def process_image_bytes(
    image_bytes: bytes,
    api_key: str = None,
    model_name: str = "gemini-3-flash-preview",
    rescale: int = None,
    vertexai: bool = False,
    project: str = None,
    location: str = "us-central1"
):
    """
    Process image bytes and extract book information.
    
    Args:
        image_bytes: Bytes of the input image
        api_key: Google API key
        model_name: Gemini model to use
        rescale: Rescale image to this max dimension
        vertexai: Whether to use Vertex AI
        project: GCP Project ID
        location: GCP Location
        
    Returns:
        Dictionary containing extracted book information
    """
    # Initialize extractor
    extractor = GeminiBookExtractor(
        api_key=api_key, 
        model_name=model_name, 
        rescale=rescale,
        vertexai=vertexai,
        project=project,
        location=location
    )
    
    # Extract books
    result = await extractor.extract_books_from_image_bytes(image_bytes)
    
    # Post-process: filter and deduplicate
    if "books" in result and result["books"]:
        raw_books = result["books"]
        
        # 1. Filter out books without titles
        filtered_books = [b for b in raw_books if b.get("title")]
        
        # 2. Apply richness-based deduplication
        deduplicated_books = BookDeduplicator.deduplicate_richness(filtered_books)
        
        result["books"] = deduplicated_books
        result["raw_books_count"] = len(raw_books)
        result["filtered_books_count"] = len(filtered_books)
        result["deduplicated_books_count"] = len(deduplicated_books)
        
        # Print summary
        count = len(deduplicated_books)
        print(f"\nFound {count} unique books (after filtering {len(raw_books) - len(filtered_books)} title-less and deduplicating {len(filtered_books) - count})")
        for i, book in enumerate(result["books"], 1):
            title = book.get("title", "Unknown")
            author = book.get("author", "Unknown")
            print(f"  {i}. {title} by {author}")
            
        if "usage" in result:
            u = result["usage"]
            print(f"\nToken Usage:")
            print(f"  Input: {u['prompt_tokens']}")
            print(f"  Output: {u['completion_tokens']}")
            print(f"  Total: {u['total_tokens']}")
    else:
        print("\nNo books found or parse error")
        
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract book information from images/videos using Google Gemini"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to input image or video"
    )
    parser.add_argument(
        "--image",
        type=str,
        help="Explicitly provide path to an input image"
    )
    parser.add_argument(
        "--video",
        type=str,
        help="Explicitly provide path to an input video"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output path (JSON for images, directory for videos)"
    )
    parser.add_argument(
        "--api_key",
        type=str,
        default="",
        help="Google API key (or set GOOGLE_API_KEY env var)"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gemini-3-flash-preview",
        help="Gemini model name (default: gemini-3-flash-preview)"
    )
    parser.add_argument(
        "--frame_interval",
        type=int,
        default=100,
        help="For videos: process every Nth frame (default: 100)"
    )
    parser.add_argument(
        "--rescale",
        type=int,
        default=None,
        help="Rescale images to this max dimension (e.g. 1600)"
    )
    parser.add_argument(
        "--vertex",
        action="store_true",
        help="Use Vertex AI instead of Gemini AI Studio"
    )
    vertex_group = parser.add_argument_group("Vertex AI Options")
    vertex_group.add_argument(
        "--project",
        type=str,
        help="GCP Project ID (required for Vertex AI)"
    )
    vertex_group.add_argument(
        "--location",
        type=str,
        default="us-central1",
        help="GCP Location for Vertex AI (default: us-central1)"
    )
    vertex_group.add_argument(
        "--region",
        type=str,
        help="Alias for --location"
    )
    
    args = parser.parse_args()
    
    # Determine location/region
    location = args.region or args.location
    
    # Determine input path
    input_path = args.input or args.image or args.video
    
    if not input_path:
        print("Error: No input provided. Please use --input, --image, or --video.")
        sys.exit(1)
    
    # Auto-detect input type unless explicitly forced via --image/--video
    video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm']
    _, ext = os.path.splitext(input_path.lower())
    
    is_video = False
    if args.video:
        is_video = True
    elif args.image:
        is_video = False
    else:
        # Default to extension check
        is_video = ext in video_extensions
    
    if is_video:
        # Process video
        output_dir = args.output if args.output else "gemini_video_output"
        process_video(
            video_path=input_path,
            output_dir=output_dir,
            frame_interval=args.frame_interval,
            api_key=args.api_key if args.api_key else None,
            model_name=args.model,
            rescale=args.rescale,
            vertexai=args.vertex,
            project=args.project,
            location=location
        )
    else:
        # Process image
        output_path = args.output if args.output else "extracted_books_gemini.json"
        process_image(
            image_path=input_path,
            output_path=output_path,
            api_key=args.api_key if args.api_key else None,
            model_name=args.model,
            rescale=args.rescale,
            vertexai=args.vertex,
            project=args.project,
            location=location
        )
