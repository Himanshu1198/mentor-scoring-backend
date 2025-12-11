"""
Cloudinary integration for video upload and storage
Replace GridFS with cloud-based video hosting
"""

import cloudinary
import cloudinary.uploader
import os
from datetime import datetime

def init_cloudinary():
    """Initialize Cloudinary with environment variables"""
    cloudinary.config(
        cloud_name=os.getenv('CLOUDINARY_CLOUD_NAME'),
        api_key=os.getenv('CLOUDINARY_API_KEY'),
        api_secret=os.getenv('CLOUDINARY_API_SECRET')
    )
    print("✓ Cloudinary initialized")


def upload_video_to_cloudinary(file_obj, mentor_id, session_id, filename):
    """
    Upload video to Cloudinary
    
    Args:
        file_obj: File-like object or binary data
        mentor_id: Mentor's ID
        session_id: Session ID
        filename: Original filename
    
    Returns:
        dict: Upload result with url and public_id
    """
    try:
        # Upload to Cloudinary with metadata in public_id
        public_id = f"mentor_videos/{mentor_id}/{session_id}"
        
        result = cloudinary.uploader.upload(
            file_obj,
            resource_type='video',
            public_id=public_id,
            folder='mentor_videos',
            overwrite=True,
            invalidate=True,
            tags=['mentor', 'session', mentor_id],
            eager=[
                {'quality': 'auto', 'fetch_format': 'auto'},  # Optimization
            ],
            eager_async=False,
            timeout=300  # 5 minutes timeout for large videos
        )
        
        print(f"✓ Video uploaded to Cloudinary: {public_id}")
        return {
            'url': result.get('secure_url'),
            'public_id': result.get('public_id'),
            'video_id': result.get('public_id'),
            'width': result.get('width'),
            'height': result.get('height'),
            'duration': result.get('duration'),
            'format': result.get('format'),
            'bytes': result.get('bytes'),
            'created_at': result.get('created_at')
        }
    except Exception as e:
        raise Exception(f'Cloudinary upload failed: {str(e)}')


def get_video_url(public_id):
    """
    Get the secure URL for a video
    
    Args:
        public_id: Cloudinary public_id
    
    Returns:
        str: Secure HTTPS URL
    """
    try:
        url = cloudinary.CloudinaryResource(public_id).build_url(
            resource_type='video',
            secure=True,
            format='auto'
        )
        return url
    except Exception as e:
        raise Exception(f'Failed to get video URL: {str(e)}')


def delete_video_from_cloudinary(public_id):
    """
    Delete a video from Cloudinary
    
    Args:
        public_id: Cloudinary public_id
    """
    try:
        cloudinary.uploader.destroy(
            public_id,
            resource_type='video'
        )
        print(f"✓ Video deleted from Cloudinary: {public_id}")
    except Exception as e:
        raise Exception(f'Cloudinary deletion failed: {str(e)}')


def get_video_metadata(public_id):
    """
    Get metadata for a video
    
    Args:
        public_id: Cloudinary public_id
    
    Returns:
        dict: Video metadata
    """
    try:
        result = cloudinary.api.resource(
            public_id,
            resource_type='video'
        )
        return {
            'public_id': result.get('public_id'),
            'url': result.get('secure_url'),
            'duration': result.get('duration'),
            'width': result.get('width'),
            'height': result.get('height'),
            'format': result.get('format'),
            'bytes': result.get('bytes'),
            'created_at': result.get('created_at')
        }
    except Exception as e:
        raise Exception(f'Failed to get metadata: {str(e)}')


def generate_signed_url(public_id, expires_in=3600):
    """
    Generate a signed URL with expiration
    
    Args:
        public_id: Cloudinary public_id
        expires_in: Expiration time in seconds (default 1 hour)
    
    Returns:
        str: Signed URL
    """
    try:
        url = cloudinary.CloudinaryResource(public_id).build_url(
            resource_type='video',
            secure=True,
            sign_url=True,
            auth_token={
                'end_time': int(datetime.utcnow().timestamp()) + expires_in,
                'duration': expires_in
            }
        )
        return url
    except Exception as e:
        raise Exception(f'Failed to generate signed URL: {str(e)}')


def get_video_info_from_url(public_id):
    """
    Get comprehensive info about a video
    
    Args:
        public_id: Cloudinary public_id
    
    Returns:
        dict: Complete video information
    """
    try:
        metadata = get_video_metadata(public_id)
        url = get_video_url(public_id)
        
        return {
            'public_id': public_id,
            'url': url,
            'metadata': metadata,
            'playback_url': url,
            'thumbnail': cloudinary.CloudinaryResource(public_id).build_url(
                resource_type='video',
                format='jpg',
                secure=True,
                page='1'  # First frame as thumbnail
            )
        }
    except Exception as e:
        raise Exception(f'Failed to get video info: {str(e)}')
